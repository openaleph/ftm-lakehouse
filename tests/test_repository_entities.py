import json

from followthemoney import model
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.repository import EntityRepository
from tests.shared import BOB, JANE, JOHN


def test_repository_entities_local(tmp_path):
    repo = EntityRepository("test", tmp_path)

    # Initially empty (check tags before query which may trigger flush)
    assert not repo._tags.exists(tag.JOURNAL_UPDATED)
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert list(repo.query(flush_first=False)) == []

    jane = make_entity(JANE)
    john = make_entity(JOHN)

    # Write entities using bulk writer
    with repo.bulk() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)

    # Tag should be set after bulk write
    assert repo._tags.exists(tag.JOURNAL_UPDATED)
    journal_updated = repo._tags.get(tag.JOURNAL_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    assert (tmp_path / "tags/lakehouse/journal/last_updated").exists()

    # Query returns entities (flushes journal first)
    # before flush:
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert repo._journal.count() > 0
    assert repo._statements.stats().entity_count == 0

    # This auto flushes the journal:
    entities = list(repo.query(flush_first=True))
    # after flush:
    assert len(entities) == 2
    assert repo._journal.count() == 0
    assert repo._statements.stats().entity_count == 2
    # Tag should be set after flush (triggered by query)
    assert repo._tags.exists(tag.STATEMENTS_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    assert (tmp_path / "tags/lakehouse/statements/last_updated").exists()

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
    with repo.bulk() as writer:
        writer.add_entity(
            make_entity(
                {"id": "bob", "schema": "Person", "properties": {"name": ["Bob"]}}
            )
        )
    assert repo._tags.get(tag.JOURNAL_UPDATED) > journal_updated


def test_repository_entities_multi_origin(tmp_path):
    """Test entity assembled from fragments with different origins."""
    repo = EntityRepository("test", tmp_path)

    # Add same entity ID from different origins with different properties
    with repo.bulk(origin="source_a") as writer:
        entity = model.make_entity("Person")
        entity.id = "multi-origin"
        entity.add("name", "John Smith")
        entity.add("nationality", "us")
        writer.add_entity(entity)

    with repo.bulk(origin="source_b") as writer:
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
    """Test incremental diff export using Delta CDC.

    Initial diff copies entities.ftm.json regardless of Delta table version.
    Subsequent diffs capture incremental changes via CDC.
    """
    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    with repo.bulk() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    assert repo._statements.version == 0

    with repo.bulk() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo._statements.version == 1

    # Export entities.ftm.json (required for initial diff)
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query(flush_first=False))

    # Initial diff - copies entities.ftm.json even though table is at v1
    diff_name_1 = repo.export_diff()
    assert diff_name_1.startswith("v1_")
    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 1  # Initial diff file created

    # Verify initial diff contains both JANE and JOHN (full export)
    with open(diff_files[0]) as f:
        lines = f.readlines()
    assert len(lines) == 2
    entities = {json.loads(line)["entity"]["id"] for line in lines}
    assert entities == {"jane", "john"}

    # Add more data: creates Delta table v2
    with repo.bulk() as writer:
        writer.add_entity(make_entity(BOB))
    repo.flush()

    # Incremental diff - captures changes from v1 to v2
    diff_name_2 = repo.export_diff()
    assert diff_name_2.startswith("v2_")
    assert diff_name_2 != diff_name_1

    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 2

    # Find and verify the incremental diff (v2) contains only BOB
    diff_files_sorted = sorted(diff_files, key=lambda p: p.name)
    with open(diff_files_sorted[1]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "bob"


def test_repository_entities_export_diff_no_changes(tmp_path):
    """Test diff export when there are no new changes after initial setup."""
    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Create data and flush
    with repo.bulk() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    assert repo._statements.version == 0

    with repo.bulk() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo._statements.version == 1

    # Export entities.ftm.json for initial diff
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query(flush_first=False))

    # Initial diff (v1) - copies entities.ftm.json
    diff_name_1 = repo.export_diff()
    assert diff_name_1.startswith("v1_")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # Only one diff file should exist (initial)
    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 1
