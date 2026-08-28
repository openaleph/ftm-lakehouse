"""Tests for ParquetStore — append-only sorted writes + async merge."""

from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
from followthemoney import Statement
from ftmq.query import M, Query
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import pack_statement
from ftmq.types import Statements

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.logic import parquet as logic_parquet
from ftm_lakehouse.model.statement import JOURNAL_SCHEMA, TABLE_RAW
from ftm_lakehouse.storage.parquet import ParquetStore

DATASET = "test"
SHARDS = 8


def make_statement(
    entity_id: str,
    prop: str,
    value: str,
    schema: str = "Person",
) -> Statement:
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema=schema,
        value=value,
        dataset=DATASET,
    )


def _pack(stmt: Statement, deleted_at: datetime | None = None) -> dict:
    """Pack a statement to a row dict with bucket, origin, deleted_at.

    ``shard`` rides along for the partition grouping in :func:`_flush` only –
    :meth:`ParquetStore.append` derives the stored one from ``entity_id``, so
    it is stripped before the table is handed over.
    """
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    row = pack_statement(stmt)
    row["first_seen"] = row.get("first_seen") or now
    row["last_seen"] = row.get("last_seen") or now
    row["shard"] = entity_shard(row["entity_id"], SHARDS)
    row["deleted_at"] = deleted_at
    row["fragment"] = ""
    return row


def _flush(store: ParquetStore, rows: list[dict]) -> int:
    """Append rows grouped by (shard, bucket, origin)."""
    by_partition: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_partition[(r["shard"], r["bucket"], r["origin"])].append(r)
    total = 0
    for (_shard, bucket, _origin), partition_rows in sorted(by_partition.items()):
        table = pa.Table.from_pylist(
            [{k: v for k, v in r.items() if k != "shard"} for r in partition_rows],
            schema=JOURNAL_SCHEMA,
        )
        store.append(table)
        total += len(table)
    return total


def _row_count(store: ParquetStore) -> int:
    """Physical row count from the raw view – pre-merge duplicates and
    tombstones included (the live view only hides tombstones)."""
    with store._lake.cursor() as cur:
        return cur.execute(f"SELECT COUNT(*) FROM {TABLE_RAW.name}").fetchone()[0]


def _get_statements(store: ParquetStore, entity_id: str) -> Statements:
    q = Query(M(entity_id=entity_id))
    yield from store.query_statements(q)


def test_storage_parquet_query_statements(tmp_path):
    """Append + query returns assembled entities and raw statements."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmts = [
        make_statement("jane", "name", "Jane Doe"),
        make_statement("jane", "firstName", "Jane"),
        make_statement("jane", "lastName", "Doe"),
        make_statement("john", "name", "John Smith"),
        make_statement("john", "firstName", "John"),
    ]
    _flush(store, [_pack(s) for s in stmts])

    entities = list(store.query())
    assert {e.id for e in entities} == {"jane", "john"}

    statements = list(store.query_statements())
    assert len(statements) == 5
    name_values = {s.value for s in statements if s.prop == "name"}
    assert name_values == {"Jane Doe", "John Smith"}


def test_storage_parquet_execute_partitioned_multiple_partitions(tmp_path):
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    # Two schemas -> two buckets (thing / interval), so at least two
    # (shard, bucket) partitions regardless of shard assignment.
    stmts = [
        make_statement("jane", "name", "Jane Doe"),
        make_statement("acme-job", "role", "CEO", schema="Membership"),
    ]
    _flush(store, [_pack(s) for s in stmts])

    assert set(store.get_entity_ids()) == {"jane", "acme-job"}


def test_storage_parquet_append_keeps_duplicates(tmp_path):
    """Append-only: re-flushing the same statement does NOT dedupe on write."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    _flush(store, [_pack(stmt)])
    _flush(store, [_pack(stmt)])

    # Two physical rows now exist; merge would collapse them.
    assert _row_count(store) == 2


def test_storage_parquet_merge_collapses_duplicates(tmp_path):
    """merge() folds duplicate statements per partition."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    r1 = _pack(stmt)
    r1["last_seen"] = datetime(2021, 6, 1, tzinfo=timezone.utc)
    r2 = _pack(stmt)
    r2["last_seen"] = datetime(2020, 6, 1, tzinfo=timezone.utc)
    _flush(store, [r1])
    _flush(store, [r2])
    assert _row_count(store) == 2

    store.merge()
    assert _row_count(store) == 1

    # Surviving row carries max last_seen
    statements = list(store.query_statements())
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt.last_seen == datetime(2021, 6, 1, tzinfo=timezone.utc)


def test_storage_parquet_merge_range_sliced(tmp_path, monkeypatch):
    """A partition estimated over the memory budget merges in sequential
    ``entity_id`` range slices – same canonical result as single-pass.

    Forces slicing by inflating the spill-factor estimate so even the tiny
    test partitions exceed the memory budget; the slice count then clamps
    to the boundary sample, exercising sampling, range construction and
    the chained reader end to end.
    """
    monkeypatch.setattr(logic_parquet, "MERGE_SPILL_FACTOR", 10**12)
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    rows = []
    for i in range(40):
        stmt = make_statement(f"e{i:02d}", "name", f"Name {i}")
        rows.append(_pack(stmt))
        rows.append(_pack(stmt))  # identical duplicate – collapses on merge
    _flush(store, rows)
    assert _row_count(store) == 80

    store.merge()
    assert _row_count(store) == 40

    entities = list(store.query())
    assert len(entities) == 40
    assert {e.id for e in entities} == {f"e{i:02d}" for i in range(40)}


def test_storage_parquet_soft_delete_hidden_after_merge(tmp_path):
    """A tombstone hides its statement once ``merge`` makes the store canonical.

    The live view is a plain ``deleted_at IS NULL`` scan with no read-time
    dedupe, so before merge the live row and the tombstone coexist and the
    live row stays visible. ``merge`` collapses the id to its tombstone (the
    latest ``last_seen``), which the live view then filters out.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    _flush(store, [_pack(stmt)])
    store.merge()
    assert len(list(store.query_statements())) == 1

    # Tombstone has a strictly LATER last_seen so merge picks it as the
    # surviving row per id; deleted_at IS NOT NULL then filters it out.
    tomb = _pack(stmt, deleted_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
    tomb["last_seen"] = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _flush(store, [tomb])

    # Before merge the live row is still visible – no read-time dedupe.
    assert len(list(store.query_statements())) == 1
    assert _row_count(store) == 2

    # Merge with grace=0 collapses the id to its tombstone and reaps both rows.
    store.settings.grace_period_days = 0
    store.merge()
    assert list(store.query_statements()) == []
    assert _row_count(store) == 0


def test_storage_parquet_merge_skips_unchanged_partitions(tmp_path):
    """merge() rewrites only partitions dirtied since their last merge.

    Asserts the freshness-tag mechanism directly: ``append`` stamps a
    ``last_updated`` tag per touched ``(shard, bucket, origin)``; ``merge``
    rewrites (and stamps ``last_optimized`` on) only partitions whose
    ``last_updated`` is newer than ``last_optimized``. So a skipped partition's
    ``last_optimized`` tag is left untouched, while a rewritten one's advances.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    # two entities in distinct shards => two `thing` partitions, each with a dup
    jane_shard = entity_shard("e-jane", SHARDS)
    john_shard = entity_shard("e-john", SHARDS)
    assert jane_shard != john_shard
    for eid in ("e-jane", "e-john"):
        _flush(store, [_pack(make_statement(eid, "name", f"{eid} v1"))])
        _flush(store, [_pack(make_statement(eid, "name", f"{eid} v1"))])
    assert _row_count(store) == 4  # two partitions x two duplicate rows

    # per-partition freshness-tag keys (bucket "thing", default origin)
    jane_opt = tag.statements_partition_optimized(jane_shard, "thing", DEFAULT_ORIGIN)
    john_opt = tag.statements_partition_optimized(john_shard, "thing", DEFAULT_ORIGIN)
    jane_upd = tag.statements_partition_updated(jane_shard, "thing", DEFAULT_ORIGIN)
    john_upd = tag.statements_partition_updated(john_shard, "thing", DEFAULT_ORIGIN)

    # before the first merge both partitions are dirty (no last_optimized yet)
    assert not store._tags.is_latest(jane_opt, [jane_upd])
    assert not store._tags.is_latest(john_opt, [john_upd])

    store.merge()
    assert _row_count(store) == 2  # both partitions collapsed
    # both partitions now optimized (last_optimized newer than last_updated)
    assert store._tags.is_latest(jane_opt, [jane_upd])
    assert store._tags.is_latest(john_opt, [john_upd])
    jane_opt_ts = store._tags.get(jane_opt)
    john_opt_ts = store._tags.get(john_opt)

    # no-op merge: nothing dirtied since last merge -> no partition re-stamped
    store.merge()
    assert store._tags.get(jane_opt) == jane_opt_ts
    assert store._tags.get(john_opt) == john_opt_ts
    assert _row_count(store) == 2

    # touch only e-jane's partition with another duplicate
    _flush(store, [_pack(make_statement("e-jane", "name", "e-jane v1"))])
    assert _row_count(store) == 3
    # e-jane is now dirty (last_updated newer than last_optimized); e-john clean
    assert not store._tags.is_latest(jane_opt, [jane_upd])
    assert store._tags.is_latest(john_opt, [john_upd])

    store.merge()
    assert _row_count(store) == 2
    # e-jane was rewritten -> its last_optimized advanced and it is clean again
    assert store._tags.get(jane_opt) > jane_opt_ts
    assert store._tags.is_latest(jane_opt, [jane_upd])
    # e-john was skipped -> its last_optimized tag is untouched
    assert store._tags.get(john_opt) == john_opt_ts


def test_storage_parquet_get_statements_uses_shard(tmp_path):
    """get_statements(entity_id) prunes to one shard subtree. This test doesn't
    validate the predicate pushdown, but the transparent logic for callers."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    _flush(
        store,
        [
            _pack(make_statement("e-jane", "name", "Jane Doe")),
            _pack(make_statement("e-john", "name", "John Smith")),
        ],
    )

    # different shards per entity
    assert entity_shard("e-jane", SHARDS) != entity_shard("e-john", SHARDS)

    jane = list(_get_statements(store, "e-jane"))
    john = list(_get_statements(store, "e-john"))
    nobody = list(_get_statements(store, "nobody"))
    assert len(jane) == 1 and jane[0].entity_id == "e-jane"
    assert len(john) == 1 and john[0].entity_id == "e-john"
    assert nobody == []
