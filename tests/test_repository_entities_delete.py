"""Soft delete: tombstones flow through journal → parquet (append) → merge.

The query view filters ``deleted_at IS NOT NULL`` *per-row*, so an entity is
hidden as soon as a tombstone row for ALL its statements lands in parquet.
``merge()`` later drops the tombstones physically once they're past the grace
cutoff.
"""

from datetime import timedelta
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy, Statement
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import make_docker_repo, make_test_api
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_local_repo(tmp_path) -> EntityRepository:
    """Create a local EntityRepository with in-memory journal."""
    return EntityRepository(DATASET, tmp_path)


def _populate(repo: EntityRepository) -> None:
    """Add two entities (jane, john) to the repo and flush to parquet."""
    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)
    with repo.writer() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    repo.flush()


@pytest.fixture(params=["local", "api", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield _make_local_repo(tmp_path), tmp_path
    elif request.param == "api":
        with make_test_api(tmp_path) as base_url:
            dataset_url = f"{base_url}/{DATASET}"
            r = get_entities(DATASET, uri=dataset_url)
            yield r, tmp_path / DATASET
    else:
        yield make_docker_repo()


def test_delete_entity_filters_from_query_after_merge(repo):
    """delete + flush + merge → entity disappears from queries.

    In append-only mode the live row and tombstone coexist after flush; the
    query view's ``deleted_at IS NULL`` filter still picks the live row.
    Merge collapses the (live, tombstone) pair to the tombstone, which the
    view then filters out.
    """
    repo, _ = repo
    _populate(repo)
    assert {e.id for e in repo.query()} == {"jane", "john"}

    count = repo.delete_entity("jane")
    assert count > 0
    repo.flush()
    repo.merge()

    assert {e.id for e in repo.query()} == {"john"}


def test_delete_entity_filters_from_stats_after_merge(repo):
    repo, _ = repo
    _populate(repo)
    assert repo.stats().entity_count == 2

    repo.delete_entity("jane")
    repo.flush()
    repo.merge()

    assert repo.stats().entity_count == 1


def test_delete_then_readd_via_merge(repo):
    """Delete, then merge; re-add lands fresh."""
    repo, _ = repo
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()
    repo.merge()
    assert {e.id for e in repo.query()} == {"john"}

    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe v2"]},
        }
    )
    with repo.writer() as writer:
        writer.add_entity(jane)
    repo.flush()

    assert {e.id for e in repo.query()} == {"jane", "john"}


def test_delete_entity_in_journal_only(repo):
    """Add + delete inside the same journal window: both rows flush, merge collapses.

    The journal is append-only – it no longer collapses a tombstone over the
    live row it shadows – so the pair reaches parquet and ``merge`` applies
    the delete, exactly as for a delete in a later window.
    """
    repo, _ = repo

    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"]},
        }
    )
    with repo.writer() as writer:
        writer.add_entity(jane)
    repo.delete_entity("jane")
    repo.merge()

    assert list(repo.query()) == []


def test_delete_nonexistent_entity(repo):
    repo, _ = repo
    _populate(repo)
    assert repo.delete_entity("nonexistent") == 0


def test_delete_statement(repo):
    """Tombstoning a single statement removes it from the live view (after merge)."""
    repo, _ = repo
    _populate(repo)

    jane_stmts = [s for s in repo.query_statements() if s.entity_id == "jane"]
    assert jane_stmts

    target = jane_stmts[0]
    repo.delete_statement(target)
    repo.flush()
    repo.merge()

    stmt_ids = {s.id for s in repo.query_statements()}
    assert target.id not in stmt_ids


def test_delete_preserves_others(repo):
    repo, _ = repo
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()
    repo.merge()

    stmts = list(repo.query_statements())
    assert stmts
    assert all(s.entity_id == "john" for s in stmts)
    assert len(stmts) > 0


# ---------------------------------------------------------------------------
# Local-only tests (access _statements internals / DeltaTable directly)
# ---------------------------------------------------------------------------


def test_delete_entity_filters_from_export_csv(tmp_path):
    """CSV export excludes deleted entities after flush + merge."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()
    repo.merge()

    repo._statements.export_csv(path.EXPORTS_STATEMENTS)

    csv_path = str(tmp_path / path.EXPORTS_STATEMENTS)
    with open(csv_path) as f:
        lines = f.readlines()

    # All lines after header should be john's statements
    for line in lines[1:]:
        assert "jane" not in line
        assert "john" in line


def test_delete_then_merge_cleans_main_table(monkeypatch, tmp_path):
    """merge() (with grace=0) physically removes deleted rows from the table."""
    monkeypatch.setenv("LAKEHOUSE_GRACE_PERIOD_DAYS", "0")  # immediate delete

    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.merge()

    # Main table should no longer contain jane's rows
    dt = repo._statements.deltatable
    raw = dt.to_pyarrow_table()
    entity_ids = set(raw.column("entity_id").to_pylist())
    assert "jane" not in entity_ids
    assert "john" in entity_ids

    deleted_at = raw.column("deleted_at").to_pylist()
    assert all(d is None for d in deleted_at)

    assert {e.id for e in repo.query()} == {"john"}


def test_deleted_at_appended_after_flush(tmp_path):
    """After delete + flush (no merge), tombstone rows exist alongside live rows."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)

    repo.delete_entity("jane")
    repo.flush()

    dt = repo._statements.deltatable
    raw = dt.to_pyarrow_table()
    deleted_rows = [r for r in raw.to_pylist() if r.get("deleted_at") is not None]
    assert deleted_rows


def _future_stmt(prop: str, value: str, entity_id: str = "acme") -> Statement:
    """A statement whose ``last_seen`` lies a year ahead of the wall clock."""
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema="Company",
        value=value,
        dataset=DATASET,
        last_seen=(utc_now() + timedelta(days=365)).isoformat(),
    )


@pytest.mark.parametrize("fragment", [None, "f"])
def test_delete_entity_dated_in_the_future(tmp_path, fragment):
    """A row dated ahead of the wall clock is still deletable.

    ``last_seen`` comes from input – a skewed crawler host, a CSV with a bad
    clock, ``--last-seen`` – so nothing bounds it by ``now``. A tombstone
    stamped with the delete time alone would rank *below* such a row in
    ``merge``'s window and be dropped instead of it, and since ``merge``
    rewrites the partition from the window's winners, the retraction record
    would be destroyed rather than merely ignored – making every retry
    identical and the entity undeletable. Both dedupe branches are exposed:
    non-fragment ranks on ``ROW_NUMBER``, fragment on the group's
    ``MAX(last_seen)``.
    """
    repo = _make_local_repo(tmp_path)
    with repo.writer() as w:
        w.add_statement(_future_stmt("name", "Acme Inc"), fragment=fragment)
    repo.flush()
    repo.merge()
    assert repo.get("acme") is not None

    repo.delete_entity("acme")
    repo.flush()
    repo.merge()

    assert repo.get("acme") is None
    assert list(repo.query_statements()) == []


def test_delete_then_readd_still_resurrects(tmp_path):
    """Tombstones stay time-ranked, so a later emission still wins.

    The guard against the future-dated row above is a ``MAX``, not "tombstones
    always win" – that would make a deleted entity unre-addable for the whole
    grace period.
    """
    repo = _make_local_repo(tmp_path)
    _populate(repo)
    repo.merge()
    repo.delete_entity("jane")
    repo.flush()
    repo.merge()
    assert repo.get("jane") is None

    with repo.writer() as w:
        w.add_entity(EntityProxy.from_dict(JANE))
    repo.flush()
    repo.merge()

    assert repo.get("jane") is not None
