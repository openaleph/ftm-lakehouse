"""Integration tests for soft delete operations via sidecar metadata table.

Deletes are written through the journal WAL. On flush, tombstones are routed
to the sidecar table (mark_deleted). SidecarAwareLakeStore joins main + sidecar
for all queries, filtering out deleted rows automatically.
"""

from pathlib import Path
from typing import Generator

import duckdb
import pytest
from followthemoney import EntityProxy

from ftm_lakehouse.api.main import archive_router, entities_router, journal_router
from ftm_lakehouse.repository.entities import EntityRepository
from tests.conftest import make_test_api
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_local_repo(tmp_path) -> EntityRepository:
    """Create a local EntityRepository with in-memory journal."""
    return EntityRepository(
        DATASET,
        tmp_path,
        journal_uri="sqlite:///:memory:",
    )


def _populate(repo: EntityRepository) -> None:
    """Add two entities (jane, john) to the repo and flush to parquet."""
    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)
    with repo.bulk() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    repo.flush()


# ---------------------------------------------------------------------------
# Parameterized fixture for tests that use only the public API
# ---------------------------------------------------------------------------


@pytest.fixture(params=["local", "api"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield _make_local_repo(tmp_path), tmp_path
    else:
        routers = [entities_router, journal_router, archive_router]
        with make_test_api(
            tmp_path, routers, journal_uri="sqlite:///:memory:"
        ) as base_url:
            dataset_url = f"{base_url}/{DATASET}"
            r = EntityRepository(DATASET, uri=dataset_url, journal_uri=dataset_url)
            yield r, tmp_path / DATASET


# ---------------------------------------------------------------------------
# Parameterized tests (local + API)
# ---------------------------------------------------------------------------


def test_delete_entity_filters_from_query(repo):
    """After delete + flush (no compact), deleted entity is excluded from queries."""
    repo, _ = repo
    _populate(repo)

    # Verify both entities exist
    entities = list(repo.query(flush_first=False))
    assert {e.id for e in entities} == {"jane", "john"}

    # Delete jane — writes tombstones to journal
    count = repo.delete_entity("jane")
    assert count > 0

    # Flush to parquet (tombstones are routed to sidecar)
    repo.flush()

    # Query should exclude jane without compact
    entities = list(repo.query(flush_first=False))
    entity_ids = {e.id for e in entities}
    assert "jane" not in entity_ids
    assert "john" in entity_ids


def test_delete_entity_filters_from_stats(repo):
    """Stats reflect live data after flush (no compact needed)."""
    repo, _ = repo
    _populate(repo)

    stats_before = repo.get_statistics()
    assert stats_before.entity_count == 2

    repo.delete_entity("jane")
    repo.flush()

    stats_after = repo.get_statistics()
    assert stats_after.entity_count == 1


def test_delete_and_readd(repo):
    """Delete then re-add: entity should be alive after flush."""
    repo, _ = repo
    _populate(repo)

    # Delete jane
    repo.delete_entity("jane")
    repo.flush()

    # Re-add jane
    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe v2"]},
        }
    )
    with repo.bulk() as writer:
        writer.add_entity(jane)
    repo.flush()

    # Jane should be alive — sidecar upsert clears deleted_at via new insert
    entities = list(repo.query(flush_first=False))
    entity_ids = {e.id for e in entities}
    assert "jane" in entity_ids
    assert "john" in entity_ids


def test_delete_entity_in_journal_only(repo):
    """Add to journal, delete before flush — nothing visible in parquet."""
    repo, _ = repo

    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"]},
        }
    )
    with repo.bulk() as writer:
        writer.add_entity(jane)

    # Delete before flush — UPSERT overwrites with deleted_at=now
    repo.delete_entity("jane")
    repo.flush()

    # Should be empty or only contain tombstones (filtered out)
    entities = list(repo.query(flush_first=False))
    assert len(entities) == 0


def test_delete_nonexistent_entity(repo):
    """Deleting a non-existent entity returns 0."""
    repo, _ = repo
    _populate(repo)

    count = repo.delete_entity("nonexistent")
    assert count == 0


def test_delete_statement(repo):
    """Delete a single statement via journal tombstone."""
    repo, _ = repo
    _populate(repo)

    stmts = list(repo.query_statements())
    jane_stmts = [s for s in stmts if s.entity_id == "jane"]
    assert len(jane_stmts) > 0

    # Delete one statement
    target = jane_stmts[0]
    repo.delete_statement(target)

    repo.flush()

    # That specific statement should be gone
    stmts_after = list(repo.query_statements())
    stmt_ids = {s.id for s in stmts_after}
    assert target.id not in stmt_ids


def test_delete_preserves_others(repo):
    """Only the targeted entity is deleted, others preserved."""
    repo, _ = repo
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()

    # John should still have all statements
    stmts = list(repo.query_statements())
    assert all(s.entity_id == "john" for s in stmts)
    assert len(stmts) > 0


# ---------------------------------------------------------------------------
# Local-only tests (access _statements internals / DeltaTable directly)
# ---------------------------------------------------------------------------


def test_delete_entity_filters_from_export_csv(tmp_path):
    """CSV export excludes deleted entities after flush (no compact needed)."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()

    csv_path = str(tmp_path / "export.csv")
    repo._statements.export_csv(csv_path)

    with open(csv_path) as f:
        lines = f.readlines()

    # All lines after header should be john's statements
    for line in lines[1:]:
        assert "jane" not in line
        assert "john" in line


def test_delete_then_compact_cleans_main_and_sidecar(tmp_path):
    """Compact applies sidecar to main table: removes deleted rows, cleans sidecar."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()

    repo._statements.compact()

    # Main table should no longer contain jane's rows
    from deltalake import DeltaTable

    dt = DeltaTable(str(repo._statements.uri))
    raw = dt.to_pyarrow_table()
    entity_ids = set(raw.column("entity_id").to_pylist())
    assert "jane" not in entity_ids
    assert "john" in entity_ids

    # Sidecar should not contain jane's entries
    sidecar_dt = repo._statements._sidecar.deltatable
    rel = duckdb.arrow(sidecar_dt.to_pyarrow_dataset())
    rows = rel.query("sc", "SELECT id FROM sc WHERE deleted_at IS NOT NULL").fetchall()
    assert len(rows) == 0

    # Queries still work
    entities = list(repo.query(flush_first=False))
    assert len(entities) == 1
    assert entities[0].id == "john"


def test_sidecar_has_deletion_entries(tmp_path):
    """After delete + flush, sidecar contains deleted_at entries."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()

    # Sidecar should exist and contain deleted_at entries for jane
    assert repo._statements._sidecar.exists
    sidecar_dt = repo._statements._sidecar.deltatable
    rel = duckdb.arrow(sidecar_dt.to_pyarrow_dataset())
    deleted = rel.query(
        "sc", "SELECT id FROM sc WHERE deleted_at IS NOT NULL"
    ).fetchall()
    assert len(deleted) > 0

    # Main table should NOT have deleted_at column
    dt = repo._statements._store.deltatable
    main_cols = {f.name for f in dt.schema().to_arrow()}
    assert "deleted_at" not in main_cols
