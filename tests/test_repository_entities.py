from followthemoney import model
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.repository import EntityRepository

JANE = {"id": "jane", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
JOHN = {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}


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
