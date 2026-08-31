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

from ftm_lakehouse.core.conventions import path, tag
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


def test_delete_entity_with_origin(repo):
    """``origin`` narrows the tombstones to that origin's rows.

    Statement ids are content-hashed and carry no origin, so the same entity
    written twice is two rows sharing an id – ``(id, origin, ...)`` is what
    keeps them apart, and what the filter selects on.
    """
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)
    for origin in ("a", "b"):
        with repo.writer(origin=origin) as writer:
            writer.add_entity(jane)
    repo.flush()
    assert {s.origin for s in repo.query_statements()} == {"a", "b"}

    count = repo.delete_entity("jane", origin="a")
    assert count > 0
    repo.flush()
    repo.merge()

    assert {s.origin for s in repo.query_statements()} == {"b"}
    assert {e.id for e in repo.query()} == {"jane"}


def test_delete_origin(repo):
    """Dropping an origin is physical – no tombstone, no merge, no grace."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)
    with repo.writer(origin="a") as writer:
        writer.add_entity(jane)
    with repo.writer(origin="b") as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    repo.flush()
    assert {e.id for e in repo.query()} == {"jane", "john"}

    repo.delete_origin("a")

    assert {s.origin for s in repo.query_statements()} == {"b"}
    assert {e.id for e in repo.query()} == {"jane", "john"}


def test_delete_origin_rejects_unsafe_name(repo):
    """A traversal sequence never reaches the predicate – api mode included."""
    repo, _ = repo
    with pytest.raises(ValueError):
        repo.delete_origin("../etc")


def test_delete_origin_rejects_quotes(repo):
    """A quoted origin never reaches the delta predicate.

    Interpolated, this origin would close the string literal and widen the
    predicate to ``origin = 'x' OR origin = 'a'`` – dropping a partition the
    caller never named. ``validate_origin`` bans the character instead of
    escaping it per site.
    """
    repo, _ = repo
    with repo.writer(origin="a") as writer:
        writer.add_entity(EntityProxy.from_dict(JANE))
    repo.flush()

    with pytest.raises(ValueError):
        repo.delete_origin("x' OR origin = 'a")

    assert {e.id for e in repo.query()} == {"jane"}


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


def test_delete_origin_flushes_journal_first(tmp_path):
    """Journalled rows are dropped too, not resurrected by the next flush.

    The drop is physical and only sees parquet, so anything still buffered
    would land *after* it. ``delete_origin`` drains the journal first.
    """
    repo = _make_local_repo(tmp_path)
    with repo.writer(origin="a") as writer:
        writer.add_entity(EntityProxy.from_dict(JANE))
    with repo.writer(origin="b") as writer:
        writer.add_entity(EntityProxy.from_dict(JOHN))
    # nothing flushed yet – both entities live only in the journal

    repo.delete_origin("a")

    assert {e.id for e in repo.query()} == {"john"}
    repo.flush()
    assert {e.id for e in repo.query()} == {"john"}


def test_delete_origin_stamps_optimized_tag(tmp_path):
    """Dropping a partition moves canonical content, so exports go stale.

    ``STATEMENTS_OPTIMIZED`` is the clock every export / statistic / diff
    keys on. A physical delete changes what a merged store says without
    ``merge`` running, so the drop stamps it itself.
    """
    repo = _make_local_repo(tmp_path)
    with repo.writer(origin="a") as writer:
        writer.add_entity(EntityProxy.from_dict(JANE))
    with repo.writer(origin="b") as writer:
        writer.add_entity(EntityProxy.from_dict(JOHN))
    repo.flush()
    repo.merge()

    # an export taken now is fresh against the merged store
    repo._tags.set(repo.EXPORTS_STATEMENTS)
    assert repo._tags.is_latest(repo.EXPORTS_STATEMENTS, [tag.STATEMENTS_OPTIMIZED])
    before = repo._tags.get(tag.STATEMENTS_OPTIMIZED)

    repo.delete_origin("a")

    assert repo._tags.get(tag.STATEMENTS_OPTIMIZED) > before
    assert not repo._tags.is_latest(repo.EXPORTS_STATEMENTS, [tag.STATEMENTS_OPTIMIZED])
    # the append-side clock is not the drop's to move – no rows landed
    assert repo._tags.get(tag.STATEMENTS_UPDATED) < before


def test_delete_origin_unknown_leaves_tags_alone(tmp_path):
    """Nothing removed, nothing invalidated – no spurious re-export."""
    repo = _make_local_repo(tmp_path)
    _populate(repo)
    repo.merge()
    before = repo._tags.get(tag.STATEMENTS_OPTIMIZED)

    repo.delete_origin("nope")

    assert repo._tags.get(tag.STATEMENTS_OPTIMIZED) == before


def test_delete_origin_blocked_by_maintenance_lock(tmp_path, monkeypatch):
    """The drop rewrites partitions, so it takes the exclusive write fence."""
    monkeypatch.setenv("LAKEHOUSE_LOCK_MAX_RETRIES", "1")
    repo = _make_local_repo(tmp_path)
    with repo.writer(origin="a") as writer:
        writer.add_entity(EntityProxy.from_dict(JANE))
    repo.flush()

    repo._statements._store.touch(path.LOCK)
    with pytest.raises(RuntimeError, match="Already locked"):
        repo.delete_origin("a")

    assert repo._statements.unlock() is True
    assert {e.id for e in repo.query()} == {"jane"}


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
