"""Tests for the operator unlock CLI / repository hook."""

import time

import pytest
from ftmq.util import make_entity
from typer.testing import CliRunner

from ftm_lakehouse.cli import cli as cli_app
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.repository.entities.main import EntityRepository
from tests.shared import BOB, JANE


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


def test_append_fence_blocked_by_maintenance_lock(tmp_path, monkeypatch) -> None:
    """An append on an existing table waits out a held ``.LOCK`` and fails
    after bounded retries instead of writing under maintenance's feet."""
    monkeypatch.setenv("LAKEHOUSE_LOCK_MAX_RETRIES", "1")
    repo = EntityRepository("test", tmp_path)
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()  # creates the table (exclusive path)

    store = repo._statements
    store._store.touch(path.LOCK)
    with repo.writer() as writer:
        writer.add_entity(make_entity(BOB))
    with pytest.raises(RuntimeError, match="Write fence busy"):
        repo.flush()
    # the parked appender must not leak its marker (it registers first,
    # then backs off marker-less while .LOCK is held)
    assert store._append_markers() == []
    assert store.unlock() is True


def test_append_leaves_fence_clear(tmp_path) -> None:
    """A successful append holds neither ``.LOCK`` nor append markers after."""
    repo = EntityRepository("test", tmp_path)
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    with repo.writer() as writer:
        writer.add_entity(make_entity(BOB))
    repo.flush()  # second flush appends via the shared fence

    store = repo._statements
    assert not store._store.exists(path.LOCK)
    assert store._append_markers() == []


def test_merge_blocked_by_stale_append_marker(tmp_path, monkeypatch) -> None:
    """Maintenance drains append markers – a stale one fails it (bounded),
    releases the exclusive lock, and clears via unlock."""
    monkeypatch.setenv("LAKEHOUSE_LOCK_MAX_RETRIES", "1")
    repo = EntityRepository("test", tmp_path)
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()

    store = repo._statements
    store._store.touch(f"{path.LOCK_APPENDS}/deadbeef")
    with pytest.raises(RuntimeError, match="append markers"):
        store.merge()
    # The exclusive lock must be released again after the failed drain.
    assert not store._store.exists(path.LOCK)

    assert store.unlock() is True
    assert store._append_markers() == []
    store.merge()  # fence is clear now


def test_unlock_clears_append_markers(tmp_path) -> None:
    repo = EntityRepository("test", tmp_path)
    store = repo._statements
    store._store.touch(f"{path.LOCK_APPENDS}/deadbeef")
    assert store.unlock() is True
    assert store.unlock() is False


def test_vacuum_requires_write_fence(tmp_path, monkeypatch) -> None:
    """vacuum acquires the dataset write fence – a held .LOCK fails it
    instead of letting it delete files under a concurrent writer's feet."""
    monkeypatch.setenv("LAKEHOUSE_LOCK_MAX_RETRIES", "1")
    repo = EntityRepository("test", tmp_path)
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()

    store = repo._statements
    store._store.touch(path.LOCK)
    with pytest.raises(RuntimeError, match="Already locked"):
        store.vacuum()
    assert store.unlock() is True


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
