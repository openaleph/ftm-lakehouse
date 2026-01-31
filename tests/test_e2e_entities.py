import csv

from followthemoney import Statement, model
from ftmq.model.stats import DatasetStats
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.operation import (
    export_entities,
    export_statements,
    export_statistics,
    optimize,
)
from tests.shared import JANE, JANE_FIRSTNAME


def test_entities(tmp_dataset):
    """Test the unified DatasetEntities interface."""
    entities = tmp_dataset.entities

    # Initially empty
    assert len([e for e in entities.query(flush_first=False)]) == 0

    jane = make_entity(JANE)
    jane_fragment = make_entity(JANE_FIRSTNAME)

    # Write entities using bulk writer
    with entities.bulk() as bulk:
        bulk.add_entity(jane)

    assert len([e for e in entities.query()]) == 1

    with entities.bulk(origin="update") as bulk:
        bulk.add_entity(jane_fragment)

    assert len([e for e in entities.query()]) == 1

    # Get entity by ID
    jane = entities.get("jane")
    assert jane is not None
    assert jane.first("name") == "Jane Doe"
    assert jane.first("firstName") == "Jane"

    # Filter by origin
    jane = entities.get("jane", origin="update")
    assert jane is not None
    assert jane.first("name") is None
    assert jane.first("firstName") == "Jane"

    # Export statements.csv
    export_statements(tmp_dataset)

    # Add a new entity to trigger re-export
    john = make_entity(
        {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}
    )
    with entities.bulk() as bulk:
        bulk.add_entity(john)
    export_statements(tmp_dataset)  # Operation's ensure_flush handles flushing

    with entities._store.open(path.EXPORTS_STATEMENTS, "r") as fh:
        reader = csv.DictReader(fh)
        data = [r for r in reader]
    assert len(data) == 6  # 2 jane (default) + 2 jane (update) + 2 john
    stmts = [Statement.from_dict(d) for d in data]
    entity_ids = set(s.entity_id for s in stmts)
    assert entity_ids == {"jane", "john"}
    origins = set(s.origin for s in stmts)
    assert origins == {"update", "default"}

    # Optimize
    optimize(tmp_dataset, vacuum=True)

    # Statistics
    export_statistics(tmp_dataset)
    stats: DatasetStats = entities._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 2  # jane and john


def test_entities_export(tmp_dataset):
    """Test entity export to JSON."""
    entities = tmp_dataset.entities
    jane = make_entity(JANE)
    jane_fragment = make_entity(JANE_FIRSTNAME)

    with entities.bulk() as bulk:
        bulk.add_entity(jane)
    with entities.bulk(origin="update") as bulk:
        bulk.add_entity(jane_fragment)

    export_statements(tmp_dataset)  # Operation's ensure_flush handles flushing
    export_entities(tmp_dataset)

    # stream() reads from exported entities.ftm.json
    ents = [e for e in entities.stream()]
    assert len(ents) == 1
    entity = ents[0]
    assert entity.id == "jane"
    assert entity.first("name") == "Jane Doe"
    assert "update" in entity.context.get("origin")


def test_entity_multi_origin_fragments(tmp_dataset):
    """Test entity assembled from fragments with different origins.

    When the same entity ID is added from multiple origins, the resulting
    entity should contain all properties and track all origins.
    """
    entities = tmp_dataset.entities

    # Add same entity ID from three different origins with different properties
    with entities.bulk(origin="source_a") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("name", "John Smith")
        entity.add("nationality", "us")
        bulk.add_entity(entity)

    with entities.bulk(origin="source_b") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("birthDate", "1980-01-15")
        entity.add("gender", "male")
        bulk.add_entity(entity)

    with entities.bulk(origin="source_c") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("email", "john@example.com")
        entity.add("nationality", "gb")  # Additional nationality
        bulk.add_entity(entity)

    # Flush and export
    entities.flush()
    export_statements(tmp_dataset)
    export_entities(tmp_dataset)

    # Query merged entity (all origins)
    merged = entities.get("multi-origin-person")
    assert merged is not None

    # Should have properties from all origins
    assert "John Smith" in merged.get("name")
    assert "1980-01-15" in merged.get("birthDate")
    assert "john@example.com" in merged.get("email")
    assert "male" in merged.get("gender")
    # Nationalities from both source_a and source_c
    nationalities = merged.get("nationality")
    assert "us" in nationalities
    assert "gb" in nationalities

    # Check origin tracking in exported entity
    exported = list(entities.stream())
    assert len(exported) == 1
    entity = exported[0]
    origins = entity.context.get("origin", [])
    assert "source_a" in origins
    assert "source_b" in origins
    assert "source_c" in origins

    # Query by single origin returns only that origin's statements
    source_a_only = entities.get("multi-origin-person", origin="source_a")
    assert source_a_only is not None
    assert "John Smith" in source_a_only.get("name")
    assert source_a_only.first("birthDate") is None  # From source_b
    assert source_a_only.first("email") is None  # From source_c


def test_entity_multi_origin_statements(tmp_dataset):
    """Test entity assembled from individual statements with different origins.

    Add statements directly via bulk writer from multiple origins
    and verify they merge correctly.
    """
    entities = tmp_dataset.entities
    dataset = tmp_dataset.name

    # Create statements directly for the same entity from different origins
    stmts_source_a = [
        Statement(
            entity_id="stmt-entity",
            prop="name",
            schema="Company",
            value="Acme Corporation",
            dataset=dataset,
        ),
        Statement(
            entity_id="stmt-entity",
            prop="jurisdiction",
            schema="Company",
            value="us",
            dataset=dataset,
        ),
    ]

    stmts_source_b = [
        Statement(
            entity_id="stmt-entity",
            prop="incorporationDate",
            schema="Company",
            value="2010-05-20",
            dataset=dataset,
        ),
        Statement(
            entity_id="stmt-entity",
            prop="status",
            schema="Company",
            value="active",
            dataset=dataset,
        ),
    ]

    stmts_source_c = [
        Statement(
            entity_id="stmt-entity",
            prop="website",
            schema="Company",
            value="https://acme.example.com",
            dataset=dataset,
        ),
    ]

    # Add statements via bulk writer with different origins
    with entities.bulk(origin="registry") as bulk:
        for stmt in stmts_source_a:
            bulk.add_statement(stmt)

    with entities.bulk(origin="filings") as bulk:
        for stmt in stmts_source_b:
            bulk.add_statement(stmt)

    with entities.bulk(origin="enrichment") as bulk:
        for stmt in stmts_source_c:
            bulk.add_statement(stmt)

    # Flush and export
    entities.flush()
    export_statements(tmp_dataset)
    export_entities(tmp_dataset)

    # Query merged entity
    merged = entities.get("stmt-entity")
    assert merged is not None
    assert merged.schema.name == "Company"

    # All properties should be present
    assert "Acme Corporation" in merged.get("name")
    assert "us" in merged.get("jurisdiction")
    assert "2010-05-20" in merged.get("incorporationDate")
    assert "active" in merged.get("status")
    assert "https://acme.example.com" in merged.get("website")

    # Verify origin tracking
    exported = list(entities.stream())
    assert len(exported) == 1
    entity = exported[0]
    origins = entity.context.get("origin", [])
    assert "registry" in origins
    assert "filings" in origins
    assert "enrichment" in origins

    # Verify statements.csv contains all origins
    with entities._store.open(path.EXPORTS_STATEMENTS, "r") as fh:
        reader = csv.DictReader(fh)
        rows = [r for r in reader]

    stmt_origins = set(r["origin"] for r in rows)
    assert stmt_origins == {"registry", "filings", "enrichment"}
    assert len(rows) == 5  # 2 + 2 + 1 statements
