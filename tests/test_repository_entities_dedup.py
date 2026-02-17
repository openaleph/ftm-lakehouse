"""Tests for flush dedup: re-adding existing entities skips main table writes
and updates translog last_seen."""

import time
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy

from ftm_lakehouse.api.main import archive_router, entities_router, journal_router
from ftm_lakehouse.repository.entities import EntityRepository
from tests.conftest import make_test_api
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_local_repo(tmp_path) -> EntityRepository:
    return EntityRepository(DATASET, tmp_path)


@pytest.fixture(params=["local", "api"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield _make_local_repo(tmp_path), tmp_path
    else:
        routers = [entities_router, journal_router, archive_router]
        with make_test_api(tmp_path, routers) as base_url:
            dataset_url = f"{base_url}/{DATASET}"
            r = EntityRepository(DATASET, uri=dataset_url)
            yield r, tmp_path / DATASET


def test_flush_dedup_no_new_rows(repo):
    """Writing the same entity twice doesn't duplicate rows in main parquet."""
    repo, _ = repo

    jane = EntityProxy.from_dict(JANE)

    # First write + flush
    repo.add(jane)
    count1 = repo.flush()
    assert count1 > 0

    # Second write + flush (same entity)
    repo.add(jane)
    count2 = repo.flush()
    assert count2 == 0  # no new rows written to main table


def test_flush_dedup_query_returns_entity(repo):
    """Entity is still queryable after dedup flush (translog join works)."""
    repo, _ = repo

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


def test_flush_dedup_mixed_new_and_existing(repo):
    """Flush with both new and existing entities: only new ones go to main table."""
    repo, _ = repo

    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)

    # First flush — only jane
    repo.add(jane)
    count1 = repo.flush()
    assert count1 > 0

    # Second flush — jane (dupe) + john (new)
    with repo.writer() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    count2 = repo.flush()

    # Only john's statements should be new
    john_stmt_count = len(
        list(s for s in repo.query_statements() if s.entity_id == "john")
    )
    assert count2 == john_stmt_count

    # Both should be queryable
    entities = list(repo.query(flush_first=False))
    entity_ids = {e.id for e in entities}
    assert entity_ids == {"jane", "john"}


def test_flush_dedup_updates_last_seen(tmp_path):
    """Re-adding an entity updates last_seen in the translog."""
    repo = _make_local_repo(tmp_path)

    jane = EntityProxy.from_dict(JANE)

    # First write + flush
    repo.add(jane)
    repo.flush()

    last_seen1 = repo.get(jane.id).last_seen

    time.sleep(2)
    # Second write + flush
    repo.add(jane)
    repo.flush()

    last_seen2 = repo.get(jane.id).last_seen
    assert last_seen1 < last_seen2
