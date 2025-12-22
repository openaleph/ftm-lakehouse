from collections import defaultdict

from followthemoney import Statement, StatementEntity, model
from followthemoney.dataset import DefaultDataset
from followthemoney.statement.serialize import read_csv_statements
from ftmq.query import Query
from ftmq.store.lake import query_duckdb
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.repository import EntityRepository

JANE = {"id": "jane", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
JOHN = {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}


def test_repository_entities_local(tmp_path):
    repo = EntityRepository("test", tmp_path)

    # Initially empty
    assert list(repo.query()) == []
    assert not repo._tags.exists(tag.JOURNAL_UPDATED)
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)

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
    entities = list(repo.query())
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
    merged = repo.get("multi-origin")
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


def test_repository_entities_export_csv(tmp_path):
    """Test exporting statements to CSV."""
    repo = EntityRepository("test", tmp_path)

    # Add entities from different origins
    with repo.bulk(origin="import") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))

    # Flush to parquet first
    repo.flush()

    # CSV should not exist yet
    csv_path = tmp_path / "exports/statements.csv"
    assert not csv_path.exists()

    # Export to CSV
    repo.export_statements()

    # CSV should exist at expected path (hardcoded to detect convention changes)
    assert csv_path.exists()

    # Deserialize CSV into Statement objects
    with open(csv_path, "rb") as fh:
        csv_statements = list(read_csv_statements(fh))

    # 2 entities × 2 statements each (id + name property)
    assert len(csv_statements) == 4

    # Get statements from parquet store via DuckDB query (not entity reconstruction)
    # This preserves the original statement IDs and origin
    deltatable = repo._statements._store.deltatable
    db = query_duckdb(Query().sql.statements, deltatable)
    parquet_statements = [
        Statement.from_dict(dict(zip(db.columns, row))) for row in db.fetchall()
    ]

    assert len(parquet_statements) == 4

    # Build lookup by statement ID for exact comparison
    csv_by_id = {s.id: s for s in csv_statements}
    parquet_by_id = {s.id: s for s in parquet_statements}

    # Statement IDs should match exactly
    assert set(csv_by_id.keys()) == set(parquet_by_id.keys())

    # All fields should match exactly
    for stmt_id, csv_stmt in csv_by_id.items():
        parquet_stmt = parquet_by_id[stmt_id]
        assert csv_stmt.id == parquet_stmt.id
        assert csv_stmt.entity_id == parquet_stmt.entity_id
        assert csv_stmt.canonical_id == parquet_stmt.canonical_id
        assert csv_stmt.schema == parquet_stmt.schema
        assert csv_stmt.prop == parquet_stmt.prop
        assert csv_stmt.value == parquet_stmt.value
        assert csv_stmt.dataset == parquet_stmt.dataset
        assert csv_stmt.origin == parquet_stmt.origin == "import"

    # Reconstruct StatementEntities from CSV and compare to original input

    # Group statements by entity_id
    statements_by_entity: dict[str, list[Statement]] = defaultdict(list)
    for stmt in csv_statements:
        statements_by_entity[stmt.entity_id].append(stmt)

    # Reconstruct entities from CSV statements
    reconstructed = {
        entity_id: StatementEntity.from_statements(DefaultDataset, stmts)
        for entity_id, stmts in statements_by_entity.items()
    }

    # Compare to original input entities
    original_entities = {"jane": JANE, "john": JOHN}
    assert set(reconstructed.keys()) == set(original_entities.keys())

    for entity_id, original in original_entities.items():
        entity = reconstructed[entity_id]
        assert entity.id == original["id"]
        assert entity.schema.name == original["schema"]
        # Compare properties (values are sets in StatementEntity)
        for prop, values in original["properties"].items():
            assert set(entity.get(prop)) == set(values)
