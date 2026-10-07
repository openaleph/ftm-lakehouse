"""Tests for ParquetStore — append-only sorted writes + async merge."""

import io
import math
import subprocess
import sys
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone

import pyarrow as pa
import pytest
from followthemoney import Statement
from ftmq.query import M, Query
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import pack_statement
from ftmq.types import Statements
from pyarrow.csv import CSVWriter  # type: ignore[attr-defined]

from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.parquet import MERGED_PREFIX, TABLE_CONFIGURATION
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
    with storage_parquet.sweep_batches(source, {}) as batches:
        list(storage_parquet.sweep_partition(batches, out))
    sql = str(statement_csv_select().compile(compile_kwargs={"literal_binds": True}))
    with storage_parquet.partition_cursor(source.relation, source.clean, {}) as cur:
        schema = cur.execute(sql).to_arrow_reader().schema
    body = out.getvalue()
    assert body and not body.startswith(statement_csv_header())

    header = io.BytesIO()
    CSVWriter(header, schema).close()
    assert header.getvalue() == statement_csv_header()


PROGRESS_BAR = """
import sys
import duckdb
from ftm_lakehouse.storage.parquet import partition_cursor
SQL = "SELECT current_setting('enable_progress_bar')"
print(duckdb.connect().execute(SQL).fetchone()[0])
with partition_cursor(sys.argv[1], sys.argv[2] == "True", {}) as cur:
    print(cur.execute(SQL).fetchone()[0])
"""


def test_storage_parquet_partition_cursor_no_progress_bar(tmp_path):
    """DuckDB draws its own progress bar over ours when it takes the process for
    an interactive session – no ``__main__.__file__`` at import, as in a spawned
    worker of the CLI, or here under ``-c`` (the first line) – unless the cursor
    turns it off."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _flush(store, _origin_rows("a"))
    source = store.sweep_sources()[0]
    out = subprocess.run(
        [sys.executable, "-c", PROGRESS_BAR, source.relation, str(source.clean)],
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
        with storage_parquet.sweep_batches(source, {}) as batches:
            rows = storage_parquet.sweep_partition(batches, io.BytesIO())
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
    with storage_parquet.sweep_batches(source, {}) as batches:
        return list(storage_parquet.sweep_partition(batches, io.BytesIO()))


def test_storage_parquet_sweep_presorted(tmp_path, monkeypatch):
    """A merged pair is swept by merging its origins' streams in file order –
    the rows sorting the pair gives, in entity order; a pair holding any other
    file is sorted."""
    monkeypatch.setattr(storage_parquet, "SWEEP_BATCH_SIZE", 7)  # cut entities
    store = ParquetStore(tmp_path, DATASET, shards=2)
    for origin, entities in (("a", 60), ("b", 60), ("c", 15)):
        _flush(store, _origin_rows(origin, entities))
    assert not any(source.presorted for source in store.sweep_sources())

    store.merge()
    sources = store.sweep_sources()
    assert sources and all(s.presorted and len(s.relations) == 3 for s in sources)
    for source in sources:
        merged = _swept(source)
        ids = [row["entity_id"] for row in merged]
        assert ids == sorted(ids)
        rows = sorted(tuple((k, str(v)) for k, v in sorted(r.items())) for r in merged)
        expected = _swept(replace(source, presorted=False))
        assert rows == sorted(
            tuple((k, str(v)) for k, v in sorted(r.items())) for r in expected
        )

    _flush(store, _origin_rows("a", 1))
    assert sum(not source.presorted for source in store.sweep_sources()) == 1


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
