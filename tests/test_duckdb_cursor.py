"""Tests for the DuckDB cursor isolation + per-connection memory limit.

``ParquetStore._lake.cursor()`` returns a thread-isolated cursor so
concurrent queries against the cached :class:`LakeStore` DuckDB
connection don't race on shared connection state.

``make_duckdb()`` plumbs ``Settings.duckdb_memory_limit`` into the
connection so a single complex query can't OOM the worker.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pyarrow as pa
from followthemoney import Statement
from ftmq.store.lake import pack_statement

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.parquet import make_duckdb
from ftm_lakehouse.model.statement import SHARDED_SCHEMA
from ftm_lakehouse.storage.parquet import ParquetStore

DATASET = "test"
SHARDS = 8


def _seed(store: ParquetStore) -> None:
    """Write a one-row batch so the Delta table exists and _duckdb / the
    registered view become usable."""
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=DATASET,
    )
    row = pack_statement(stmt)
    row["first_seen"] = now
    row["last_seen"] = now
    row["shard"] = entity_shard(row["canonical_id"], SHARDS)
    row["deleted_at"] = None
    store.append(pa.Table.from_pylist([row], schema=SHARDED_SCHEMA))


def test_cursor_isolation_under_concurrent_reads(tmp_path) -> None:
    """Concurrent threaded queries against one ParquetStore use independent
    cursors and don't collide on the shared cached connection."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _seed(store)

    def _hit_duckdb(i: int) -> int:
        with store._lake.cursor() as cur:
            (n,) = cur.execute("SELECT ?", [i]).fetchone()
            return n

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(_hit_duckdb, range(128)))

    assert results == list(range(128))


def test_cursor_can_query_registered_view(tmp_path) -> None:
    """Cursors inherit the loaded Delta extension and the registered view
    from the parent connection, so they can read the statement store
    without any per-cursor setup."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _seed(store)

    with store._lake.cursor() as cur:
        (n,) = cur.execute("SELECT COUNT(*) FROM statement").fetchone()
    assert n == 1


def test_make_duckdb_applies_memory_limit(monkeypatch) -> None:
    """``Settings.duckdb_memory_limit`` flows into the new connection."""
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_MEMORY_LIMIT", "256MB")
    assert Settings().duckdb_memory_limit == "256MB"

    con = make_duckdb()
    (limit,) = con.execute("SELECT current_setting('memory_limit')").fetchone()
    # DuckDB normalises to IEC units, so "256MB" comes back as "244.1 MiB".
    assert "MiB" in limit or "MB" in limit


def test_make_duckdb_applies_temp_directory(monkeypatch, tmp_path) -> None:
    """``Settings.duckdb_temp_directory`` flows into the new connection."""
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    assert Settings().duckdb_temp_directory == str(tmp_path)

    con = make_duckdb()
    (configured,) = con.execute("SELECT current_setting('temp_directory')").fetchone()
    assert configured == str(tmp_path)


def test_make_duckdb_default_temp_directory_unset(monkeypatch) -> None:
    """When unset, ``temp_directory`` is not forced – DuckDB picks its own
    default rather than being told to spill to an empty path."""
    monkeypatch.delenv("LAKEHOUSE_DUCKDB_TEMP_DIRECTORY", raising=False)
    assert Settings().duckdb_temp_directory is None
    # Constructor must not raise.
    make_duckdb()
