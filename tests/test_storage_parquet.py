"""Tests for ParquetStore — append-only sorted writes + async merge."""

from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
from followthemoney import Statement
from ftmq.store.lake import pack_statement

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.model.statement import SHARDED_SCHEMA, TABLE
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
    """Pack a statement to a row dict with shard, bucket, origin, deleted_at."""
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    row = pack_statement(stmt)
    row["first_seen"] = row.get("first_seen") or now
    row["last_seen"] = row.get("last_seen") or now
    row["shard"] = entity_shard(row["canonical_id"], SHARDS)
    row["deleted_at"] = deleted_at
    return row


def _flush(store: ParquetStore, rows: list[dict]) -> int:
    """Append rows grouped by (shard, bucket, origin)."""
    by_partition: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_partition[(r["shard"], r["bucket"], r["origin"])].append(r)
    total = 0
    for (_shard, bucket, _origin), partition_rows in sorted(by_partition.items()):
        table = pa.Table.from_pylist(partition_rows, schema=SHARDED_SCHEMA)
        store.append(table)
        total += len(table)
    return total


def _row_count(store: ParquetStore) -> int:
    return store._duckdb.execute(f"SELECT COUNT(*) FROM {TABLE.name}").fetchone()[0]


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


def test_storage_parquet_soft_delete_visible_until_merge(tmp_path):
    """Tombstones coexist with live rows in append-only mode.

    The query view filters ``deleted_at IS NULL`` per-row, but in append-only
    mode the live row is still present alongside the tombstone — so the live
    row remains visible to queries until ``merge()`` collapses the pair.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    _flush(store, [_pack(stmt)])
    assert len(list(store.query_statements())) == 1

    # Tombstone has a strictly LATER last_seen so merge picks it as the
    # surviving row per id.
    tomb = _pack(stmt, deleted_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
    tomb["last_seen"] = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _flush(store, [tomb])

    # Live row still visible (tombstone is a separate, filtered-out row)
    assert len(list(store.query_statements())) == 1

    # Merge collapses the pair to the latest version (the tombstone); with
    # grace=0 it then drops the tombstone entirely.
    store.merge(grace_period_days=0)
    assert len(list(store.query_statements())) == 0


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

    jane = list(store.get_statements("e-jane"))
    john = list(store.get_statements("e-john"))
    nobody = list(store.get_statements("nobody"))
    assert len(jane) == 1 and jane[0].entity_id == "e-jane"
    assert len(john) == 1 and john[0].entity_id == "e-john"
    assert nobody == []
