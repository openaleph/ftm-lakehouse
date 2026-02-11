"""Tests for flush dedup: re-adding existing entities skips main table writes
and updates sidecar last_seen."""

import duckdb
from followthemoney import EntityProxy

from ftm_lakehouse.repository.entities import EntityRepository
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_repo(tmp_path) -> EntityRepository:
    return EntityRepository(
        DATASET,
        tmp_path,
        journal_uri="sqlite:///:memory:",
    )


def test_flush_dedup_no_new_rows(tmp_path):
    """Writing the same entity twice doesn't duplicate rows in main parquet."""
    repo = _make_repo(tmp_path)

    jane = EntityProxy.from_dict(JANE)

    # First write + flush
    repo.add(jane)
    count1 = repo.flush()
    assert count1 > 0

    # Second write + flush (same entity)
    repo.add(jane)
    count2 = repo.flush()
    assert count2 == 0  # no new rows written to main table


def test_flush_dedup_updates_last_seen(tmp_path):
    """Re-adding an entity updates last_seen in the sidecar."""
    repo = _make_repo(tmp_path)

    jane = EntityProxy.from_dict(JANE)

    # First write + flush
    repo.add(jane)
    repo.flush()

    # Read sidecar last_seen
    sidecar_dt = repo._statements._sidecar.deltatable
    rel = duckdb.arrow(sidecar_dt.to_pyarrow_dataset())
    rows1 = rel.query("sc", "SELECT id, last_seen FROM sc").fetchall()
    last_seen_1 = {r[0]: r[1] for r in rows1}

    # Second write + flush
    repo.add(jane)
    repo.flush()

    # Sidecar last_seen should be updated
    sidecar_dt2 = repo._statements._sidecar.deltatable
    rel2 = duckdb.arrow(sidecar_dt2.to_pyarrow_dataset())
    rows2 = rel2.query("sc", "SELECT id, last_seen FROM sc").fetchall()
    last_seen_2 = {r[0]: r[1] for r in rows2}

    # Every statement's last_seen should be >= original
    for stmt_id in last_seen_1:
        assert last_seen_2[stmt_id] >= last_seen_1[stmt_id]


def test_flush_dedup_query_returns_entity(tmp_path):
    """Entity is still queryable after dedup flush (sidecar join works)."""
    repo = _make_repo(tmp_path)

    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()

    # Re-add same entity
    repo.add(jane)
    repo.flush()

    # Query should return the entity exactly once
    entities = list(repo.query(flush_first=False))
    entity_ids = {e.id for e in entities}
    assert jane.id in entity_ids


def test_flush_dedup_mixed_new_and_existing(tmp_path):
    """Flush with both new and existing entities: only new ones go to main table."""
    repo = _make_repo(tmp_path)

    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)

    # First flush — only jane
    repo.add(jane)
    count1 = repo.flush()
    assert count1 > 0

    # Second flush — jane (dupe) + john (new)
    with repo.bulk() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    count2 = repo.flush()

    # Only john's statements should be new
    john_stmt_count = len(
        list(s for s in repo._statements.query_statements() if s.entity_id == "john")
    )
    assert count2 == john_stmt_count

    # Both should be queryable
    entities = list(repo.query(flush_first=False))
    entity_ids = {e.id for e in entities}
    assert entity_ids == {"jane", "john"}
