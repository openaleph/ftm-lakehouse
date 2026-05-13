import json
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import model
from ftmq.util import make_entity

from ftm_lakehouse.api.main import archive_router, entities_router, journal_router
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.repository import EntityRepository
from tests.conftest import make_docker_repo, make_test_api
from tests.shared import BOB, JANE, JANE_FIRSTNAME, JOHN


@pytest.fixture(params=["local", "api", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield EntityRepository("test", tmp_path), tmp_path
    elif request.param == "api":
        routers = [entities_router, journal_router, archive_router]
        with make_test_api(tmp_path, routers) as base_url:
            dataset_url = f"{base_url}/test"
            repo = EntityRepository("test", uri=dataset_url)
            yield repo, tmp_path / "test"
    else:
        # docker: real nginx + lakehouse UDS; data lives at
        # ``./data/{dataset}`` on the host via the bind mount, so tests
        # that assert on the on-disk layout still work.
        yield make_docker_repo()


def test_repository_entities(repo):
    repo, base_path = repo

    # Initially empty (check tags before query which may trigger flush)
    assert not repo._tags.exists(tag.JOURNAL_UPDATED)
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert list(repo.query()) == []

    jane = make_entity(JANE)
    john = make_entity(JOHN)

    # Write entities using bulk writer
    with repo.writer() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)

    # Tag should be set after bulk write
    assert repo._tags.exists(tag.JOURNAL_UPDATED)
    journal_updated = repo._tags.get(tag.JOURNAL_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/journal/last_updated").exists()

    # Query returns entities (flushes journal first)
    # before flush:
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert repo._journal.count() > 0
    assert repo.get_statistics().entity_count == 0

    # This auto flushes the journal:
    entities = list(repo.query(flush_first=True))
    # after flush:
    assert len(entities) == 2
    assert repo._journal.count() == 0
    assert repo.get_statistics().entity_count == 2
    # Tag should be set after flush (triggered by query)
    assert repo._tags.exists(tag.STATEMENTS_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/statements/last_updated").exists()

    # Get entity by ID
    jane_entity = repo.get("jane")
    assert jane_entity is not None
    assert jane_entity.first("name") == "Jane Doe"

    john_entity = repo.get("john")
    assert john_entity is not None
    assert john_entity.first("name") == "John Doe"

    # Non-existent entity returns None
    assert repo.get("nobody") is None

    # Adding more entities updates the journal tag
    with repo.writer() as writer:
        writer.add_entity(
            make_entity(
                {"id": "bob", "schema": "Person", "properties": {"name": ["Bob"]}}
            )
        )
    assert repo._tags.get(tag.JOURNAL_UPDATED) > journal_updated


def test_repository_entities_multi_origin(repo):
    """Test entity assembled from fragments with different origins."""
    repo, _ = repo

    # Add same entity ID from different origins with different properties
    with repo.writer(origin="source_a") as writer:
        entity = model.make_entity("Person")
        entity.id = "multi-origin"
        entity.add("name", "John Smith")
        entity.add("nationality", "us")
        writer.add_entity(entity)

    with repo.writer(origin="source_b") as writer:
        entity = model.make_entity("Person")
        entity.id = "multi-origin"
        entity.add("birthDate", "1980-01-15")
        entity.add("nationality", "gb")
        writer.add_entity(entity)

    # Query merged entity (all origins)
    merged = repo.get("multi-origin", flush_first=True)
    assert merged is not None
    assert "John Smith" in merged.get("name")
    assert "1980-01-15" in merged.get("birthDate")
    nationalities = merged.get("nationality")
    assert "us" in nationalities
    assert "gb" in nationalities

    # Query by single origin returns only that origin's statements
    source_a_only = repo.get("multi-origin", origin="source_a")
    assert source_a_only is not None
    assert "John Smith" in source_a_only.get("name")
    assert source_a_only.first("birthDate") is None


def test_repository_entities_export_diff(tmp_path):
    """Test incremental diff export using change detection.

    Initial diff copies entities.ftm.json regardless of Delta table version.
    Subsequent diffs capture incremental changes via translog timestamps.

    Sleeps cross second boundaries because FtM truncates timestamps to seconds
    and diff detection uses first_seen >= floor(since).
    """
    import time

    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    assert repo.version == 0

    with repo.writer() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo.version == 1

    # Export entities.ftm.json (required for initial diff)
    smart_write_proxies(repo._store.open(path.ENTITIES_JSON, "wb"), repo.query())

    # Cross second boundary so initial entities are in an earlier second
    time.sleep(1.1)

    # Initial diff - copies entities.ftm.json even though table is at v1
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")  # timestamp format
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 1  # Initial diff file created

    # Verify initial diff contains both JANE and JOHN (full export)
    with repo._store.open(diff_files[0]) as f:
        lines = f.readlines()
    assert len(lines) == 2
    entities = {json.loads(line)["entity"]["id"] for line in lines}
    assert entities == {"jane", "john"}

    # Add more data: creates Delta table v2
    with repo.writer() as writer:
        writer.add_entity(make_entity(BOB))
    repo.flush()

    # Cross second boundary so BOB's first_seen is before next diff state
    time.sleep(1.1)

    # Incremental diff - captures changes via translog
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 2

    # Find and verify the incremental diff contains only BOB
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[1]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "bob"

    # Re-adding jane without changes doesn't create new diff after merge
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    repo.merge()

    assert repo.export_diff() is None
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 2

    # Updating Jane firstName creates diff
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE_FIRSTNAME))
    repo.flush()

    diff_name_3 = repo.export_diff()
    assert diff_name_3 is not None
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 3

    # Find and verify the incremental diff contains only JANE
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[2]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "jane"


def test_repository_entities_export_diff_delete(tmp_path):
    """Test that deleting an entity produces a DEL op in the incremental diff."""
    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Add two entities and flush
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()

    # Export entities.ftm.json (required for initial diff)
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query())

    # Initial diff
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None

    # Delete jane, flush tombstones to parquet, then merge with the default
    # 7-day grace: live rows are collapsed away (so jane is invisible to
    # queries) but tombstones survive (so the diff sees the deleted_at >=
    # since signal and emits DEL).
    repo.delete_entity("jane")
    repo.flush()
    repo.merge()

    # Incremental diff should contain a DEL for jane
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = sorted(
        (tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"),
        key=lambda p: p.name,
    )
    assert len(diff_files) == 2

    # Read the incremental diff (second file)
    with open(diff_files[1]) as f:
        lines = f.readlines()

    ops = [json.loads(line) for line in lines]
    del_ops = [o for o in ops if o["op"] == "DEL"]
    assert len(del_ops) == 1
    assert del_ops[0]["entity"]["id"] == "jane"


def test_repository_entities_export_diff_no_changes(tmp_path):
    """Test diff export when there are no new changes after initial setup."""
    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Create data and flush
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    assert repo.version == 0

    with repo.writer() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo.version == 1

    # Export entities.ftm.json for initial diff
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query())

    # Initial diff - copies entities.ftm.json
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # Only one diff file should exist (initial)
    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 1
