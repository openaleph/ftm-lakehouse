"""Tests for the operator unlock CLI / repository hook."""

import time

import pytest
from typer.testing import CliRunner

from ftm_lakehouse.cli import cli as cli_app
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.repository.entities.main import EntityRepository


def test_parquet_store_unlock_releases_lock(tmp_path) -> None:
    """ParquetStore.unlock removes the .LOCK file and reports the state."""
    repo = EntityRepository("test", tmp_path)
    store = repo._statements

    # No lock yet → unlock is a no-op.
    assert store.unlock() is False

    # Acquire and abandon the lock (simulate a crashed writer).
    store._store.touch(path.LOCK)
    assert store._store.exists(path.LOCK)

    assert store.unlock() is True
    assert not store._store.exists(path.LOCK)

    # Second call is again a no-op.
    assert store.unlock() is False


def test_entity_repository_unlock_delegates(tmp_path) -> None:
    repo = EntityRepository("test", tmp_path)
    repo._statements._store.touch(path.LOCK)
    assert repo.unlock() is True
    assert repo.unlock() is False


def test_write_lock_bounded_acquisition(tmp_path, monkeypatch) -> None:
    """A held .LOCK fails writers after bounded retries instead of hanging."""
    monkeypatch.setenv("LAKEHOUSE_LOCK_MAX_RETRIES", "1")
    repo = EntityRepository("test", tmp_path)
    store = repo._statements
    store._store.touch(path.LOCK)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="Already locked"):
        with store._write_lock():
            pass
    # One retry sleeps ~1–2s; anything near this bound means we hung.
    assert time.monotonic() - started < 10

    # Failing to acquire must not release the holder's lock.
    assert store._store.exists(path.LOCK)


@pytest.fixture()
def cli_runner(tmp_path, monkeypatch) -> CliRunner:
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    # The CLI memoises the catalog / dataset on first invocation, so wipe
    # the module-level state between tests to keep the new URI honoured.
    from ftm_lakehouse import cli as cli_module
    from ftm_lakehouse.lake import get_lakehouse

    cli_module.STATE["catalog"] = None
    cli_module.STATE["dataset"] = None
    get_lakehouse.cache_clear()
    return CliRunner()


def test_cli_unlock_releases_held_lock(tmp_path, cli_runner) -> None:
    repo = EntityRepository("scratch", tmp_path / "scratch")
    repo._statements._store.touch(path.LOCK)

    result = cli_runner.invoke(cli_app, ["-d", "scratch", "operations", "unlock"])
    assert result.exit_code == 0, result.output
    assert "released" in result.output.lower()
    assert not repo._statements._store.exists(path.LOCK)


def test_cli_unlock_noop_when_no_lock(tmp_path, cli_runner) -> None:
    EntityRepository("scratch", tmp_path / "scratch")  # initialise dataset dir

    result = cli_runner.invoke(cli_app, ["-d", "scratch", "operations", "unlock"])
    assert result.exit_code == 0, result.output
    assert "no lock" in result.output.lower()
