"""Tests for the DuckDB cursor isolation + per-connection memory limit.

``ParquetStore._lake.cursor()`` returns a thread-isolated cursor so
concurrent queries against the cached :class:`LakeStore` DuckDB
connection don't race on shared connection state.

``make_duckdb()`` plumbs ``Settings.duckdb_memory_limit`` into the
connection so a single complex query can't OOM the worker.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from tempfile import gettempdir

import duckdb
import pyarrow as pa
import pytest
from followthemoney import Statement
from ftmq.store.lake import pack_statement

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.parquet import duckdb_config, worker_duckdb_config
from ftm_lakehouse.model.statement import JOURNAL_SCHEMA
from ftm_lakehouse.storage.parquet import ParquetStore
from tests.duck import make_duckdb

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
    row["deleted_at"] = None
    row["fragment"] = ""
    store.append(pa.Table.from_pylist([row], schema=JOURNAL_SCHEMA))


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


def test_cursor_session_is_utc(tmp_path) -> None:
    """The LakeStore session renders TIMESTAMPTZ in UTC, whatever the host
    zone – cursors inherit it from the parent connection."""
    store = ParquetStore(tmp_path, DATASET, shards=SHARDS)
    _seed(store)

    with store._lake.cursor() as cur:
        (tz,) = cur.execute("SELECT current_setting('TimeZone')").fetchone()
    assert tz == "UTC"


def test_duckdb_config_connects_without_installable_extensions(
    monkeypatch, tmp_path
) -> None:
    """The config must not need an extension DuckDB would have to install.

    The Docker image points ``extension_directory`` at a read-only dir that
    only holds ``delta``. A connect-time ``TimeZone`` option is applied before
    the statically linked ``icu`` registers, so DuckDB tries to install
    ``icu`` there and the connect fails.
    """
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY", str(tmp_path))
    config = {**duckdb_config(), "autoinstall_known_extensions": "false"}
    duckdb.connect(":memory:", config=config).close()


def test_make_duckdb_applies_memory_limit(monkeypatch) -> None:
    """``Settings.duckdb_memory_limit`` flows into the new connection."""
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_MEMORY_LIMIT", "256MB")
    assert Settings().duckdb_memory_limit == "256MB"

    con = make_duckdb()
    (limit,) = con.execute("SELECT current_setting('memory_limit')").fetchone()
    # DuckDB normalises to IEC units, so "256MB" comes back as "244.1 MiB".
    assert "MiB" in limit or "MB" in limit


@pytest.mark.parametrize("limit", ["80%", "max", ""])
def test_settings_reject_memory_limit_share(monkeypatch, limit) -> None:
    """The limit is split between workers, so it must be a byte size."""
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_MEMORY_LIMIT", limit)
    with pytest.raises(ValueError):
        Settings()


def test_settings_reject_no_workers(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_WORKERS", "0")
    with pytest.raises(ValueError):
        Settings()


def test_worker_duckdb_config_splits_memory_limit(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_MEMORY_LIMIT", "1GiB")
    assert worker_duckdb_config(4)["memory_limit"] == f"{2**28}B"


def test_make_duckdb_applies_temp_directory(monkeypatch, tmp_path) -> None:
    """``Settings.duckdb_temp_directory`` flows into the new connection, as the
    parent of its own spill directory."""
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    assert Settings().duckdb_temp_directory == str(tmp_path)

    con = make_duckdb()
    (configured,) = con.execute("SELECT current_setting('temp_directory')").fetchone()
    assert os.path.dirname(configured) == str(tmp_path)


def test_duckdb_config_spill_directory_per_instance(monkeypatch, tmp_path) -> None:
    """Every config spills into its own directory – DuckDB instances number
    their spill files alike, so two sharing one overwrite each other's blocks
    (``BrokenProcessPool`` / ``Corrupt temporary file`` under parallel merge).
    The configured parent is created, as DuckDB only creates the leaf."""
    base = tmp_path / "missing" / "duckdb"
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_TEMP_DIRECTORY", str(base))
    first, second = duckdb_config(), duckdb_config()
    assert first["temp_directory"] != second["temp_directory"]
    assert os.path.dirname(first["temp_directory"]) == str(base)
    assert base.is_dir()


def test_make_duckdb_default_temp_directory(monkeypatch) -> None:
    """When unset, ``temp_directory`` is in the OS default"""
    monkeypatch.delenv("LAKEHOUSE_DUCKDB_TEMP_DIRECTORY", raising=False)
    tmp = gettempdir()
    assert Settings().duckdb_temp_directory.startswith(tmp)
    # Constructor must not raise.
    make_duckdb()
