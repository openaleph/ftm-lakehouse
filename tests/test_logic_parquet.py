"""Tests for the DuckDB merge query in ``ftm_lakehouse.logic.parquet``."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from ftm_lakehouse.logic.parquet import build_merge_query
from ftm_lakehouse.model.statement import SHARDED_SCHEMA, TABLE_RAW
from tests.duck import make_duckdb


def _table(rows: list[dict]) -> pa.Table:
    cols: dict[str, list] = {f.name: [] for f in SHARDED_SCHEMA}
    for r in rows:
        for k in cols:
            cols[k].append(r.get(k))
    return pa.table(cols, schema=SHARDED_SCHEMA)


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)


def _run(
    table: pa.Table, *, shard: str, bucket: str, origin: str, grace_cutoff: datetime
):
    con = make_duckdb()
    con.register(TABLE_RAW.name, table)
    q = build_merge_query(shard, bucket, origin, grace_cutoff)
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    return con.execute(sql).to_arrow_table()


def test_merge_collapses_duplicates(now):
    """Two rows with the same id collapse to the row with latest last_seen."""
    early = now - timedelta(hours=1)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": early,
                "last_seen": early,
                "deleted_at": None,
            },
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert out.num_rows == 1
    row = out.to_pylist()[0]
    # first_seen folded to the min across the id group; last_seen kept as max
    assert row["first_seen"] == early
    assert row["last_seen"] == now


def test_merge_drops_old_tombstone(now):
    """Tombstone older than the grace cutoff is dropped."""
    old = now - timedelta(days=14)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": old,
                "last_seen": old,
                "deleted_at": old,
            }
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 0


def test_merge_keeps_recent_tombstone(now):
    """Tombstone newer than the grace cutoff is kept."""
    recent = now - timedelta(days=1)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": recent,
                "last_seen": recent,
                "deleted_at": recent,
            }
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 1


def test_merge_filters_to_partition(now):
    """Rows outside the target partition are not selected."""
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
            {
                "id": "s2",
                "entity_id": "e2",
                "shard": "1",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert out.num_rows == 1
    assert out.to_pylist()[0]["id"] == "s1"


def test_merge_query_composable_with_where(now):
    """Consumers can add `.where()` to narrow further (e.g. a single entity)."""
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "alice",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
            {
                "id": "s2",
                "entity_id": "bob",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    con = make_duckdb()
    con.register(TABLE_RAW.name, table)
    q = build_merge_query("0", "thing", "ingest", now)
    q = q.where(q.selected_columns.entity_id == "alice")
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    out = con.execute(sql).to_arrow_table()
    assert out.num_rows == 1
    assert out.to_pylist()[0]["entity_id"] == "alice"


def test_merge_output_sorted(now):
    """Output rows are sorted by (entity_id, id, last_seen DESC)."""
    table = _table(
        [
            {
                "id": "z",
                "entity_id": "e2",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
            {
                "id": "a",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    rows = out.to_pylist()
    assert [r["entity_id"] for r in rows] == ["e1", "e2"]
