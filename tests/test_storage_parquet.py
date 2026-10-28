"""Tests for ParquetStore — append-only sorted writes + async merge."""

import io
import math
import random
import subprocess
import sys
from collections import Counter, defaultdict
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from followthemoney import Statement
from ftmq.query import M, Query
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import pack_statement
from ftmq.types import Statements
from pyarrow.csv import CSVWriter  # type: ignore[attr-defined]
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.parquet import (
    MERGED_PREFIX,
    STAGING_DIR,
    TABLE_CONFIGURATION,
    build_merge_sql,
    duckdb_config,
)
from ftm_lakehouse.model.statement import (
    JOURNAL_SCHEMA,
    TABLE_RAW,
    statement_csv_header,
    statement_csv_select,
)
from ftm_lakehouse.storage import parquet as storage_parquet
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


def _journal_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(
        [{k: v for k, v in r.items() if k != "shard"} for r in rows],
        schema=JOURNAL_SCHEMA,
    )


def _raw_rows(store: ParquetStore) -> list[tuple]:
    with store._lake.cursor() as cur:
        return cur.execute(f"SELECT * FROM {TABLE_RAW.name} ORDER BY ALL").fetchall()


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


def test_storage_parquet_query_spans_partitions(tmp_path):
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    # Two schemas -> two buckets (thing / interval), so at least two
    # (shard, bucket) partitions regardless of shard assignment.
    stmts = [
        make_statement("jane", "name", "Jane Doe"),
        make_statement("acme-job", "role", "CEO", schema="Membership"),
    ]
    _flush(store, [_pack(s) for s in stmts])

    assert {s.entity_id for s in store.query_statements()} == {"jane", "acme-job"}


def test_storage_parquet_append_keeps_duplicates(tmp_path):
    """Append-only: re-flushing the same statement does NOT dedupe on write."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    _flush(store, [_pack(stmt)])
    _flush(store, [_pack(stmt)])

    # Two physical rows now exist; merge would collapse them.
    assert _row_count(store) == 2


def test_storage_parquet_append_stamps_after_commit(tmp_path, monkeypatch):
    """``statements/last_updated`` postdates the commit: an export starting while
    an append writes misses its rows, so it must not count as fresh."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    write = storage_parquet.write_deltalake
    started: list[datetime] = []

    def write_deltalake(*args, **kwargs):
        started.append(utc_now())  # an export starting mid-append
        return write(*args, **kwargs)

    monkeypatch.setattr(storage_parquet, "write_deltalake", write_deltalake)
    _flush(store, _origin_rows("a"))
    assert store._tags.get(tag.STATEMENTS_UPDATED) > started[-1]


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


def test_storage_parquet_soft_delete_hidden(tmp_path):
    """A tombstone hides its statement as soon as it lands.

    The live row and the tombstone coexist physically until a merge; the read
    collapses the id to its tombstone (the latest ``last_seen``) and filters
    it out. ``merge`` with grace ``0`` then reaps both rows.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)

    stmt = make_statement("jane", "name", "Jane Doe")
    _flush(store, [_pack(stmt)])
    assert len(list(store.query_statements())) == 1

    tomb = _pack(stmt, deleted_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
    tomb["last_seen"] = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _flush(store, [tomb])
    assert list(store.query_statements()) == []
    assert _row_count(store) == 2

    store.settings.grace_period_days = 0
    store.merge()
    assert list(store.query_statements()) == []
    assert _row_count(store) == 0


def _partition_files(store: ParquetStore) -> dict[tuple[str, str, str], list[str]]:
    """Active data file basenames per partition, from the snapshot."""
    actions = pa.table(store.deltatable.get_add_actions(flatten=True))
    files: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for file, shard, bucket, origin in zip(
        actions["path"].to_pylist(),
        actions["partition.shard"].to_pylist(),
        actions["partition.bucket"].to_pylist(),
        actions["partition.origin"].to_pylist(),
    ):
        files[(shard, bucket, origin)].append(file.rsplit("/", 1)[-1])
    return dict(files)


def test_storage_parquet_create_adds_no_file(tmp_path):
    """Table creation is an empty commit – no data file, nothing dirty."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    store._ensure_table()
    assert store.exists
    assert store._partitions()[1] == {}
    assert not store.needs_merge


def _dirty(store: ParquetStore) -> set:
    return {p for p, (_, clean) in store._partitions()[1].items() if not clean}


def test_storage_parquet_merge_skips_clean_partitions(tmp_path):
    """merge() rewrites only dirty partitions – those holding a file it did
    not write. The signal is the snapshot's file list: merge output is named
    ``merged-*``, everything else (delta-rs appends) ``part-*``; no tags.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    jane_shard = entity_shard("e-jane", SHARDS)
    john_shard = entity_shard("e-john", SHARDS)
    assert jane_shard != john_shard
    for eid in ("e-jane", "e-john"):
        _flush(store, [_pack(make_statement(eid, "name", f"{eid} v1"))])
        _flush(store, [_pack(make_statement(eid, "name", f"{eid} v1"))])
    jane = (jane_shard, "thing", DEFAULT_ORIGIN)
    john = (john_shard, "thing", DEFAULT_ORIGIN)
    assert _row_count(store) == 4
    assert _dirty(store) == {jane, john}
    assert store.needs_merge

    store.merge()
    files = _partition_files(store)
    assert _row_count(store) == 2
    assert all(f.startswith(MERGED_PREFIX) for names in files.values() for f in names)
    assert not store.needs_merge
    version = store.version

    # nothing dirty: no partition rewritten, no commit
    store.merge()
    assert store.version == version
    assert _partition_files(store) == files

    # a duplicate into e-jane's partition dirties that one alone
    _flush(store, [_pack(make_statement("e-jane", "name", "e-jane v1"))])
    assert _dirty(store) == {jane}
    assert _row_count(store) == 3

    store.merge()
    after = _partition_files(store)
    assert _row_count(store) == 2
    assert after[john] == files[john]  # skipped – untouched
    assert after[jane] != files[jane]
    assert all(f.startswith(MERGED_PREFIX) for f in after[jane])

    # force rewrites clean partitions too
    store.merge(force=True)
    assert _partition_files(store)[john] != after[john]
    assert _row_count(store) == 2


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


def _origin_rows(origin: str, entities: int = 20) -> list[dict]:
    """Every statement twice – a merge has duplicates to collapse."""
    rows = []
    for i in range(entities):
        for _ in range(2):
            row = _pack(make_statement(f"e{i}", "name", f"Name {i}"))
            row["origin"] = origin
            rows.append(row)
    return rows


def _files_per_partition(store: ParquetStore) -> dict[tuple[str, str, str], int]:
    actions = pa.table(store.deltatable.get_add_actions(flatten=True))
    counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for key in zip(
        actions["partition.shard"].to_pylist(),
        actions["partition.bucket"].to_pylist(),
        actions["partition.origin"].to_pylist(),
    ):
        counts[key] += 1
    return dict(counts)


def test_storage_parquet_merge_escaped_origin(tmp_path):
    """An origin delta-rs percent-escapes in the partition path merges in
    place: the merged file lands in the partition's own directory, and every
    file it was merged from is removed by the same commit."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    origin = "mapping:abc 1"
    _flush(store, _origin_rows(origin))
    _flush(store, _origin_rows(origin))
    assert _row_count(store) == 80
    assert set(_files_per_partition(store).values()) == {2}

    store.merge()

    assert _row_count(store) == 20
    assert set(_files_per_partition(store).values()) == {1}
    assert {s.origin for s in store.query_statements()} == {origin}
    assert not store.needs_merge


def test_storage_parquet_sweep_header(tmp_path):
    """`statement_csv_header` is the header pyarrow writes for the sweep's
    columns, and a swept part carries none."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))

    source = store.sweep_sources()[0]
    out = io.BytesIO()
    with storage_parquet.sweep_pair(source, {}, out) as rows:
        list(rows)
    sql = str(statement_csv_select().compile(compile_kwargs={"literal_binds": True}))
    partition = source.partitions[0]
    with storage_parquet._connect({}) as con:
        storage_parquet.register_partition(con, partition.relation, False)
        schema = con.execute(sql).to_arrow_reader().schema
    body = out.getvalue()
    assert body and not body.startswith(statement_csv_header())

    header = io.BytesIO()
    CSVWriter(header, schema).close()
    assert header.getvalue() == statement_csv_header()


PROGRESS_BAR = """
import duckdb
from ftm_lakehouse.storage.parquet import _connect
SQL = "SELECT current_setting('enable_progress_bar')"
print(duckdb.connect().execute(SQL).fetchone()[0])
with _connect({}) as con:
    print(con.execute(SQL).fetchone()[0])
"""


def test_storage_parquet_connect_no_progress_bar():
    """DuckDB draws its own progress bar over ours when it takes the process for
    an interactive session – no ``__main__.__file__`` at import, as in a spawned
    worker of the CLI, or here under ``-c`` (the first line) – unless the
    connection turns it off."""
    out = subprocess.run(
        [sys.executable, "-c", PROGRESS_BAR],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.split() == ["True", "False"]


def test_storage_parquet_sweep_sources_cover_every_row(tmp_path):
    """The pairs the export sweep fans out over are the whole store, once."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    _flush(store, _origin_rows("b"))

    sources = store.sweep_sources()

    assert len(sources) > 1, "a sharded store must have several pairs to fan out"
    per_pair = []
    for source in sources:
        with storage_parquet.sweep_pair(source, {}, io.BytesIO()) as rows:
            per_pair.append(list(rows))
    # same rows as a query, and every entity in exactly one pair
    swept = sorted(r["id"] for rows in per_pair for r in rows)
    assert swept == sorted(s.id for s in store.query_statements())
    entities = [{r["entity_id"] for r in rows} for rows in per_pair]
    assert not set.intersection(*entities)
    # and every file's bytes, for the bar's throughput
    _, partitions = store._partitions()
    files = sum(size for fs, _ in partitions.values() for _, size in fs)
    assert sum(source.size for source in sources) == files > 0


def _swept(source: storage_parquet.SweepSource) -> list[dict]:
    with storage_parquet.sweep_pair(source, {}, io.BytesIO()) as rows:
        return list(rows)


def test_storage_parquet_sweep(tmp_path, monkeypatch):
    """Every partition of a pair is read on its own and the rows merged in
    entity order – a merged one in file order, any other sorted through the
    dedupe – giving the rows the store reads, whatever mix a pair holds."""
    monkeypatch.setattr(storage_parquet, "SWEEP_BATCH_SIZE", 7)  # cut entities
    store = ParquetStore(tmp_path, DATASET, shards=2)
    for origin, entities in (("a", 60), ("b", 60), ("c", 15)):
        _flush(store, _origin_rows(origin, entities))
    tombstone = _pack(
        make_statement("e0", "name", "Name 0"),
        deleted_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    _flush(store, [{**tombstone, "origin": "a"}])

    def check(presorted: set[bool]) -> None:
        sources = store.sweep_sources()
        assert {p.presorted for s in sources for p in s.partitions} == presorted
        assert all(len(source.partitions) == 3 for source in sources)
        swept = []
        for source in sources:
            rows = _swept(source)
            ids = [row["entity_id"] for row in rows]
            assert ids == sorted(ids)
            swept.extend(rows)
        expected = [(s.id, s.origin) for s in store.query_statements()]
        assert sorted((r["id"], r["origin"]) for r in swept) == sorted(expected)

    check({False})  # duplicates and a tombstone, deduped per partition
    store.merge()
    check({True})  # one sorted file each
    _flush(store, _origin_rows("a", 1))
    check({True, False})


def test_storage_parquet_connections_set_up_storage(tmp_path, monkeypatch):
    """Merge and re-shard open their DuckDB through `_connect`, so they get the
    lake's storage secret – an S3 lake's files are unreadable without it."""
    setups = []
    setup = storage_parquet.setup_duckdb_storage
    monkeypatch.setattr(
        storage_parquet,
        "setup_duckdb_storage",
        lambda con: setups.append(con) or setup(con),
    )
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))

    store.merge()
    assert setups, "merge connected without the storage setup"
    setups.clear()
    store.shard(2)
    assert setups, "re-shard connected without the storage setup"


def test_storage_parquet_merge_workers(tmp_path, monkeypatch):
    """Merging in worker processes gives what merging in-process gives."""
    merged = []
    for workers in (1, 2):
        monkeypatch.setenv("LAKEHOUSE_WORKERS", str(workers))
        store = ParquetStore(tmp_path / str(workers), DATASET, shards=SHARDS)
        _flush(store, _origin_rows("a"))
        _flush(store, _origin_rows("b"))
        store.merge()
        merged.append(sorted((s.id, s.origin) for s in store.query_statements()))
    assert merged[0] == merged[1]
    assert len(merged[0]) == 40


def test_storage_parquet_merge_commits_before_failure(tmp_path, monkeypatch):
    """A failing partition – or a dead worker – ends the merge, but what finished
    before it is committed, so the next run merges only the rest."""
    monkeypatch.setenv("LAKEHOUSE_WORKERS", "1")
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    _flush(store, _origin_rows("b"))
    merge_partition = storage_parquet.merge_partition
    calls: list[tuple[str, str, str]] = []
    fail_at: int | None = 4

    def record(task):
        calls.append(task.partition)
        if len(calls) == fail_at:
            raise RuntimeError("worker died")
        return merge_partition(task)

    monkeypatch.setattr(storage_parquet, "merge_partition", record)
    with pytest.raises(RuntimeError, match="worker died"):
        store.merge()
    partitions = store._partitions()[1]
    assert len(partitions) > 4
    assert sorted(p for p, (_, clean) in partitions.items() if clean) == sorted(
        calls[:3]
    )

    calls.clear()
    fail_at = None
    store.merge()
    assert len(calls) == len(partitions) - 3
    assert not store.needs_merge
    assert _row_count(store) == 40


def test_storage_parquet_merge_spill_directory_per_task(tmp_path, monkeypatch):
    """Each partition's DuckDB instance spills into its own directory – workers
    sharing one crash on each other's spill files."""
    monkeypatch.setenv("LAKEHOUSE_WORKERS", "2")  # in-process below, split config
    monkeypatch.setattr(
        storage_parquet, "process_map", lambda *_, **__: nullcontext(map)
    )
    spill = []
    merge_partition = storage_parquet.merge_partition

    def record(task):
        spill.append(task.duckdb_config["temp_directory"])
        return merge_partition(task)

    monkeypatch.setattr(storage_parquet, "merge_partition", record)
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    store.merge()
    assert len(spill) == len(store._partitions()[1]) > 1
    assert len(set(spill)) == len(spill)


@pytest.mark.parametrize("workers", (1, 2))
def test_storage_parquet_merge_commit_batches(tmp_path, monkeypatch, workers):
    """Merged partitions commit in batches – a Delta version per batch, not
    per partition. The pool changes who produces the results, not how the
    parent commits them."""
    monkeypatch.setattr(storage_parquet, "MERGE_COMMIT_BATCH", 3)
    monkeypatch.setenv("LAKEHOUSE_WORKERS", str(workers))
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    partitions = len(store._partitions()[1])
    assert partitions > 3
    version = store.version

    store.merge()

    assert store.version - version == math.ceil(partitions / 3)
    assert _row_count(store) == 20


def test_storage_parquet_table_configuration(tmp_path):
    """A new store carries the log-bounding table properties, so configuring
    it again changes nothing."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    config = store.deltatable.metadata().configuration
    assert {k: config.get(k) for k in TABLE_CONFIGURATION} == TABLE_CONFIGURATION
    assert store.configure_table() == {}


def _orphans(store: ParquetStore, root) -> set[str]:
    """Data files on disk the current snapshot does not reference."""
    snapshot = store._current_snapshot()
    assert snapshot is not None
    live = {uri.rsplit("/", 1)[-1] for uri in snapshot.file_uris()}
    on_disk = {p.name for p in root.rglob("*.parquet") if "_delta_log" not in p.parts}
    return on_disk - live


def test_storage_parquet_vacuum_reaps_orphans_a_checkpoint_forgot(tmp_path):
    """A file whose ``remove`` aged out of the checkpoint is still reaped.

    ``delta.deletedFileRetentionDuration`` keeps a ``remove`` in checkpoints
    for an hour (`TABLE_CONFIGURATION`), and
    [`merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge] ends on a
    checkpoint – so a vacuum that walked the log for removes instead of
    listing the table would find nothing after it, and the orphan would sit
    on disk for good.
    """
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    rows = [_pack(make_statement("jane", "name", "Jane Doe"))]
    _flush(store, rows)
    _flush(store, rows)  # a second file per partition for merge to replace
    store.merge()
    forgotten = _orphans(store, tmp_path)
    assert forgotten

    # what the one-hour retention does to every remove older than it once the
    # next checkpoint is written
    store.deltatable.alter.set_table_properties(
        {"delta.deletedFileRetentionDuration": "interval 0 hours"}
    )
    _flush(store, rows)
    store.merge()
    assert forgotten <= _orphans(store, tmp_path)

    store.vacuum()
    assert not _orphans(store, tmp_path)


def test_storage_parquet_lookup_queries_its_partitions(tmp_path, monkeypatch):
    """An id lookup reads only the ``(shard, bucket)`` pairs its prune
    allows, all in one query; a query that cannot prune (an OR) still reads
    every pair, one query each."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    pairs = sorted({(s, b) for s, b, _ in store._partitions()[1]})
    assert len(pairs) > 2

    executed = []
    cursor_over = store._cursor_over

    def spy(source, clean):
        executed.append(source)
        return cursor_over(source, clean)

    monkeypatch.setattr(store, "_cursor_over", spy)

    assert {s.entity_id for s in _get_statements(store, "e1")} == {"e1"}
    assert len(executed) == 1

    executed.clear()
    q = Query(M(entity_id__in=["e1", "e2"]))
    assert {s.entity_id for s in store.query_statements(q)} == {"e1", "e2"}
    assert len(executed) == 1
    assert {entity_shard(e, SHARDS) for e in ("e1", "e2")} == {
        shard for shard, _ in pairs if f"shard={shard}/" in executed[0]
    }

    executed.clear()
    q = Query(M(entity_id="e1") | M(entity_id="e2"))
    assert {s.entity_id for s in store.query_statements(q)} == {"e1", "e2"}
    assert len(executed) == len(pairs)


def test_storage_parquet_shard_escaped_origin(tmp_path):
    """A re-shard reads an origin delta-rs percent-escapes in the partition
    path from the snapshot's files and moves every row to its new shard."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    origin = "mapping:abc 1"
    _flush(store, _origin_rows(origin))
    store.shard(3)
    assert _row_count(store) == 40
    assert store.needs_merge  # the re-shard writes part-* files
    entity_ids = {s.entity_id for s in store.query_statements()}
    shards = {shard for shard, _, _ in store._partitions()[1]}
    assert shards == {entity_shard(e, 3) for e in entity_ids}
    store.merge()
    assert {s.origin for s in store.query_statements()} == {origin}
    assert _row_count(store) == 20


def test_storage_parquet_shard_reads_in_bounded_batches(tmp_path, monkeypatch):
    """A re-shard hands delta-rs batches of `_batch_rows` rows, not DuckDB's
    default of a million – a document group's strings overflowed Arrow's 2GiB
    offset limit in one such batch."""
    monkeypatch.setattr(storage_parquet, "MERGE_BATCH_ROWS", 5)
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    sizes: list[int] = []
    write = storage_parquet.write_deltalake

    def record(table, data, **kwargs):
        batches = list(data)
        sizes.extend(b.num_rows for b in batches)
        reader = pa.RecordBatchReader.from_batches(data.schema, batches)
        return write(table, reader, **kwargs)

    monkeypatch.setattr(storage_parquet, "write_deltalake", record)
    store.shard(2)
    assert sizes and max(sizes) <= 5 and sum(sizes) == 40
    assert _row_count(store) == 40


def test_storage_parquet_merge_commits_across_a_racing_append(tmp_path, monkeypatch):
    """An append that lands while a merge runs is kept: the merge removes
    only the files it read, the append only adds, Delta commits both, and the
    read reconciles the result. The appended file leaves the partition dirty
    for the next merge."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    other = ParquetStore(tmp_path, DATASET, shards=SHARDS)  # another process
    _flush(store, _origin_rows("a", entities=1))  # e0 twice
    partition = (entity_shard("e0", SHARDS), "thing", "a")

    commit = store._commit_merged

    def racing_commit(batch):
        rows = [_pack(make_statement("e0", "name", "Name 0"))]  # a third copy
        rows.append(_pack(make_statement("e1", "name", "Name 1")))  # a new entity
        for row in rows:
            row["origin"] = "a"
        _flush(other, rows)
        commit(batch)

    monkeypatch.setattr(store, "_commit_merged", racing_commit)
    store.merge()

    files = _partition_files(store)[partition]
    assert sum(f.startswith(MERGED_PREFIX) for f in files) == 1
    assert sum(f.startswith("part-") for f in files) == 1  # the racing e0 copy
    assert store.needs_merge
    statements = sorted((s.entity_id, s.value) for s in store.query_statements())
    assert statements == [("e0", "Name 0"), ("e1", "Name 1")]
    assert _row_count(store) == 3  # merged e0, appended e0, appended e1

    monkeypatch.setattr(store, "_commit_merged", commit)  # no more racing
    store.merge()
    assert not store.needs_merge
    assert _row_count(store) == 2


def test_storage_parquet_merge_writes_a_checkpoint(tmp_path):
    """A merge that committed ends with a Delta checkpoint, so the next load
    does not replay a checkpoint still listing every file the merge removed."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    log = tmp_path / "statements" / "_delta_log"
    assert not list(log.glob("*.checkpoint.parquet"))

    store.merge()
    checkpoints = list(log.glob("*.checkpoint.parquet"))
    assert [int(c.name.split(".")[0]) for c in checkpoints] == [store.version]

    store.merge()  # nothing dirty, nothing committed, no new checkpoint
    assert list(log.glob("*.checkpoint.parquet")) == checkpoints


def _written(store: ParquetStore, root) -> dict[tuple[str, str, str], list[tuple]]:
    """Per partition, every active file's per-column parquet profile (encodings,
    compression, bloom filter) and its Delta ``add`` stats."""
    actions = pa.table(store.deltatable.get_add_actions(flatten=True))
    stats = [
        c
        for c in actions.column_names
        if c.split(".")[0] in ("num_records", "null_count", "min", "max")
    ]
    written: dict[tuple[str, str, str], list[tuple]] = defaultdict(list)
    with storage_parquet._connect({}) as con:
        for row in actions.to_pylist():
            profile = con.execute(
                "SELECT path_in_schema, string_agg(DISTINCT encodings ORDER BY "
                "encodings), any_value(compression), bool_or(bloom_filter_offset "
                "IS NOT NULL) FROM parquet_metadata(?) GROUP BY ALL ORDER BY ALL",
                [str(root / row["path"])],
            ).fetchall()
            partition = tuple(
                row[f"partition.{p}"] for p in ("shard", "bucket", "origin")
            )
            written[partition].append((profile, [row[c] for c in stats]))
    return dict(written)


def test_storage_parquet_merge_writes_like_an_append(tmp_path):
    """A merged file is written as an append writes it: the bucket's writer
    profile – dictionaries and bloom filters per column – and delta-rs's full
    ``add`` stats, so reads of a merged store skip files as before."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    rows = [_pack(make_statement(f"e{i}", "name", f"Name {i}")) for i in range(20)]
    rows += [
        _pack(make_statement(f"p{i}", "bodyText", f"Page {i} " * 50, "Page"))
        for i in range(20)
    ]
    _flush(store, rows)
    root = tmp_path / "statements"
    appended = _written(store, root)
    assert {bucket for _, bucket, _ in appended} == {"thing", "page"}

    store.merge()
    assert not store.needs_merge
    assert _written(store, root) == appended


def test_storage_parquet_merge_chunks(tmp_path, monkeypatch):
    """A partition merges into one file per `MERGE_CHUNK_BYTES` chunk, whose
    names sort in entity order – the sweep reads them unsorted – and leaves no
    staging table behind."""
    monkeypatch.setattr(storage_parquet, "MERGE_BATCH_ROWS", 7)
    monkeypatch.setattr(storage_parquet, "MERGE_CHUNK_BYTES", 1)  # a file a batch
    store = ParquetStore(tmp_path, DATASET, shards=1)
    _flush(store, _origin_rows("a", 30))

    store.merge()
    files = _partition_files(store)[("0", "thing", "a")]
    assert len(files) == 5  # 30 rows, 7 a batch
    assert all(f.startswith(MERGED_PREFIX) for f in files)
    assert not list((tmp_path / "statements" / STAGING_DIR).iterdir())
    (source,) = store.sweep_sources()
    assert [p.presorted for p in source.partitions] == [True]
    ids = [row["entity_id"] for row in _swept(source)]
    assert ids == sorted(ids)
    assert len(ids) == 30


def test_storage_parquet_merge_force_resorts(tmp_path, monkeypatch):
    """``force`` re-sorts the last merge's files: one out of order comes out in
    entity order."""
    monkeypatch.setattr(storage_parquet, "MERGE_BATCH_ROWS", 7)
    monkeypatch.setattr(storage_parquet, "MERGE_DEDUPE_BYTES", 1)  # a chunk a batch
    store = ParquetStore(tmp_path, DATASET, shards=1)
    _flush(store, _origin_rows("a", 30))
    chunks = storage_parquet._chunks

    def rotated(batches, size):
        # one file, its 14 highest rows first: two whole read batches, which a
        # merge trusting the order writes out before the lower rows arrive
        whole = pa.concat_batches(list(batches))
        cut = whole.num_rows - 14
        yield pa.concat_batches([whole.slice(cut), whole.slice(0, cut)])

    monkeypatch.setattr(storage_parquet, "_chunks", rotated)
    store.merge()
    monkeypatch.setattr(storage_parquet, "_chunks", chunks)

    def swept() -> list[str]:
        (source,) = store.sweep_sources()
        assert [p.presorted for p in source.partitions] == [True]
        return [row["entity_id"] for row in _swept(source)]

    assert swept() != sorted(swept())
    assert not store.needs_merge  # clean: only `force` rewrites it
    store.merge(force=True)
    ids = swept()
    assert ids == sorted(ids)
    assert len(ids) == 30


def _whole_partition_merge(store: ParquetStore, grace_days: int) -> list[tuple]:
    """What the dedupe query gives over all files of every partition at once –
    the rows a merge has to produce."""
    cutoff = utc_now() - timedelta(days=grace_days)
    root, partitions = store._partitions()
    rows: list[tuple] = []
    with storage_parquet._connect(duckdb_config()) as con:
        for partition, (files, _) in partitions.items():
            source = storage_parquet._relation(root, partition, files)
            rows += con.execute(build_merge_sql(*partition, cutoff, source)).fetchall()
    return rows


def _random_rows(rng: random.Random, now: datetime, count: int) -> list[dict]:
    """Duplicates, fragment groups, roles and tombstones inside and past a
    30-day grace, in two origins."""
    rows = []
    for _ in range(count):
        entity = f"e{rng.randrange(40)}"
        prop, value = rng.choice([("name", "A"), ("name", "B"), ("alias", "C")])
        row = _pack(make_statement(entity, prop, value))
        row["origin"] = rng.choice(["a", "b"])
        row["role"] = rng.choice([None, "r1"])
        row["fragment"] = rng.choice(["", "", "f1", "f2"])
        row["first_seen"] = now - timedelta(days=rng.randrange(100))
        row["last_seen"] = now - timedelta(days=rng.randrange(100))
        if rng.random() < 0.25:
            row["deleted_at"] = now - timedelta(days=rng.choice([5, 60]))
        rows.append(row)
    return rows


def test_storage_parquet_merge_matches_the_whole_partition_query(tmp_path, monkeypatch):
    """Merged in runs and entity chunks – both cut tiny here, so every batch is
    a run of its own, the runs merge in rounds of two and nearly every entity
    is a chunk – a partition holds row for row what the dedupe query gives over
    all of its files at once, and its files read sorted. A second round merges
    the first's output with new files, the new ones as one run."""
    monkeypatch.setattr(storage_parquet, "MERGE_RUN_BYTES", 1)
    monkeypatch.setattr(storage_parquet, "MERGE_DEDUPE_BYTES", 1)
    monkeypatch.setattr(storage_parquet, "MERGE_BATCH_ROWS", 5)
    monkeypatch.setattr(storage_parquet, "MERGE_MAX_RUNS", 2)
    runs: list[str] = []
    read_run = storage_parquet._read_run
    monkeypatch.setattr(
        storage_parquet,
        "_read_run",
        lambda path, rows: runs.append(path) or read_run(path, rows),
    )
    rng = random.Random(3)
    now = utc_now()
    store = ParquetStore(tmp_path, DATASET, shards=2)
    store.settings.grace_period_days = 30
    rows = _random_rows(rng, now, 150)
    for i in range(0, len(rows), 30):  # several files per partition
        _flush(store, rows[i : i + 30])

    def check() -> None:
        expected = _whole_partition_merge(store, 30)
        assert expected
        store.merge()
        assert not store.needs_merge
        assert Counter(_raw_rows(store)) == Counter(expected)
        for source in store.sweep_sources():
            assert all(p.presorted for p in source.partitions)
            ids = [row["entity_id"] for row in _swept(source)]
            assert ids == sorted(ids)

    check()
    assert any("run-1-" in run for run in runs)  # runs merged in rounds
    assert any(deleted for *_, deleted in _raw_rows(store))  # tombstones in grace

    runs.clear()
    monkeypatch.setattr(storage_parquet, "MERGE_RUN_BYTES", 10**12)
    _flush(store, _random_rows(rng, now, 60))
    check()
    assert runs and all("run-0-" in run for run in runs)  # one run, no rounds


def test_storage_parquet_destroy(tmp_path, monkeypatch):
    """Drops the log, under ``LAKEHOUSE_STATEMENTS_URI`` too; the files stay
    until the re-created table is vacuumed."""
    monkeypatch.setenv("LAKEHOUSE_STATEMENTS_URI", str(tmp_path / "ssd"))
    store = ParquetStore(tmp_path / "hdd", DATASET, shards=SHARDS)
    table = tmp_path / "ssd" / DATASET / "statements"
    _flush(store, _origin_rows("a"))
    files = {p for p in table.rglob("*.parquet") if "_delta_log" not in p.parts}
    assert files
    updated = store._tags.set(tag.STATEMENTS_UPDATED)

    store.destroy()
    assert not store.exists
    assert not (table / "_delta_log").exists()
    assert all(p.exists() for p in files)
    assert not list(store.query())
    assert store._tags.get(tag.STATEMENTS_UPDATED) > updated
    store.destroy()  # nothing left to destroy

    _flush(store, _origin_rows("b", entities=5))
    assert _row_count(store) == 10
    store.vacuum()
    assert not any(p.exists() for p in files)
    assert _row_count(store) == 10


def test_storage_parquet_vacuum_drops_staging(tmp_path):
    """What a crashed merge staged goes with the next vacuum – Delta's own
    vacuum never lists the hidden directory."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    staged = tmp_path / "statements" / STAGING_DIR / "crashed"
    staged.mkdir(parents=True)
    (staged / "part-0.parquet").write_bytes(b"x")

    store.vacuum()
    assert not staged.parent.exists()
    assert _row_count(store) == 40


def test_storage_parquet_write_stream(tmp_path):
    """One file per partition a stream spans – not one per table – with the
    rows `append` writes, and a checkpoint at the end."""
    rows = _origin_rows("a", 30) + _origin_rows("b", 30)
    rows += [
        _pack(make_statement(f"p{i}", "bodyText", f"Page {i}", "Page"))
        for i in range(30)
    ]
    tables = [_journal_table(rows[i::3]) for i in range(3)]

    streamed = ParquetStore(tmp_path / "stream", DATASET, shards=2)
    assert streamed.write_stream(iter(tables)) == len(rows)
    appended = ParquetStore(tmp_path / "append", DATASET, shards=2)
    for table in tables:
        appended.append(table)

    assert _raw_rows(streamed) == _raw_rows(appended)
    files = _partition_files(streamed)
    assert files.keys() == _partition_files(appended).keys()
    assert all(len(fs) == 1 for fs in files.values())
    assert streamed.version == 2  # one commit per bucket
    assert sum(map(len, _partition_files(appended).values())) > len(files)
    log = tmp_path / "stream" / "statements" / "_delta_log"
    assert list(log.glob("*.checkpoint.parquet"))


def test_storage_parquet_write_stream_flushes(tmp_path, monkeypatch):
    """A partition is written when its rows fill a file, or – the budget
    exceeded – the largest first."""
    store = ParquetStore(tmp_path, DATASET, shards=1)
    big = _journal_table(_origin_rows("big", 40))
    small = _journal_table(_origin_rows("small", 5))
    seen: list[dict] = []

    def tables():
        yield pa.concat_tables([big, small])
        seen.append(_files_per_partition(store))
        yield small

    store.write_stream(tables(), budget=3 * small.nbytes)
    assert seen == [{("0", "thing", "big"): 1}]
    assert _files_per_partition(store) == {
        ("0", "thing", "big"): 1,
        ("0", "thing", "small"): 1,
    }

    monkeypatch.setattr(storage_parquet, "MERGE_CHUNK_BYTES", 1)  # a file a part
    store = ParquetStore(tmp_path / "rotate", DATASET, shards=1)
    store.write_stream([small, small])
    assert _files_per_partition(store) == {("0", "thing", "small"): 2}


def test_storage_parquet_write_stream_commits_per_call(tmp_path, monkeypatch):
    """A flush writes a bucket's partitions in one commit per
    `WRITE_FLUSH_PARTITIONS`."""
    monkeypatch.setattr(storage_parquet, "WRITE_FLUSH_PARTITIONS", 5)
    store = ParquetStore(tmp_path, DATASET, shards=8)
    rows = _origin_rows("a", 200) + _origin_rows("b", 200)
    store.write_stream([_journal_table(rows)])
    files = _files_per_partition(store)
    assert len(files) == 16  # 8 shards x 2 origins
    assert all(count == 1 for count in files.values())
    assert store.version == 4  # 16 partitions, 5 a call


def test_storage_parquet_write_stream_budget_setting(tmp_path, monkeypatch):
    """The budget defaults to ``LAKEHOUSE_WRITE_BUFFER``, a byte size."""
    monkeypatch.setenv("LAKEHOUSE_WRITE_BUFFER", "80%")
    with pytest.raises(ValueError):
        ParquetStore(tmp_path, DATASET)
    monkeypatch.setenv("LAKEHOUSE_WRITE_BUFFER", "1KiB")
    store = ParquetStore(tmp_path, DATASET, shards=1)
    big = _journal_table(_origin_rows("big", 40))
    small = _journal_table(_origin_rows("small", 5))
    seen: list[dict] = []

    def tables():
        yield pa.concat_tables([big, small])
        seen.append(_files_per_partition(store))
        yield small

    store.write_stream(tables())
    assert seen == [{("0", "thing", "big"): 1, ("0", "thing", "small"): 1}]


def test_storage_parquet_by_partition_copies(tmp_path):
    """A part holds its own rows only – dropping the others frees them."""
    store = ParquetStore(tmp_path, DATASET, shards=4)
    baseline = pa.total_allocated_bytes()
    table = _journal_table(_origin_rows("a", 500))
    parts = dict(store._by_partition(table))
    assert len(parts) == 4
    assert sum(len(p) for p in parts.values()) == len(table)
    assert all(
        set(pc.unique(p["shard"]).to_pylist()) == {shard}
        for (shard, _, _), p in parts.items()
    )
    nbytes = table.nbytes
    kept = parts.popitem()[1]
    del table, parts
    assert pa.total_allocated_bytes() - baseline < nbytes // 2  # one part's worth
    assert len(kept) > 0


def test_storage_parquet_statements_uri(tmp_path, monkeypatch):
    """``LAKEHOUSE_STATEMENTS_URI`` moves the table – merge staging included –
    while locks and tags stay with the dataset."""
    ssd, hdd = tmp_path / "ssd", tmp_path / "hdd"
    monkeypatch.setenv("LAKEHOUSE_STATEMENTS_URI", str(ssd))
    store = ParquetStore(hdd, DATASET, shards=SHARDS)
    table = ssd / DATASET / "statements"
    _flush(store, _origin_rows("a"))
    store.merge()
    assert (table / "_delta_log").is_dir()
    assert list(table.glob("shard=*"))
    assert not (hdd / "statements").exists()
    assert (hdd / "tags").is_dir()
    with store.merge_lock():
        assert (hdd / ".LOCK-MERGE").exists()

    staged = table / STAGING_DIR / "crashed"
    staged.mkdir(parents=True)
    store.vacuum()
    assert not staged.parent.exists()
    assert _row_count(store) == 20

    with pytest.raises(ValueError):
        ParquetStore(hdd, "../escape")
