import csv
from typing import Generator

import pytest
from followthemoney import Statement, model
from ftmq.model.stats import DatasetStats
from ftmq.util import make_entity

from ftm_lakehouse.api.main import (
    archive_router,
    entities_router,
    journal_router,
    operations_router,
)
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.operation import (
    export_entities,
    export_statements,
    export_statistics,
    merge,
)
from tests.conftest import (
    LAKEHOUSE_TEST_URL,
    make_docker_dataset_name,
    make_test_api,
    skip_unless_docker_mode,
)
from tests.shared import JANE, JANE_FIRSTNAME


@pytest.fixture(params=["local", "api", "docker"])
def dataset(request, tmp_path) -> Generator[Dataset, None, None]:
    if request.param == "local":
        lake = get_lakehouse(tmp_path)
        yield lake.get_dataset("test")
    elif request.param == "api":
        routers = [entities_router, journal_router, operations_router, archive_router]
        with make_test_api(tmp_path, routers) as base_url:
            lake = get_lakehouse(base_url)
            yield lake.get_dataset("test")
    else:
        # docker: real nginx fronting the lakehouse Granian UDS. Unique
        # dataset name keeps concurrent / repeated runs isolated.
        skip_unless_docker_mode()
        lake = get_lakehouse(LAKEHOUSE_TEST_URL)
        yield lake.get_dataset(make_docker_dataset_name())


def test_entities(dataset):
    """Test the unified DatasetEntities interface."""
    entities = dataset.entities

    # Initially empty
    assert len([e for e in entities.query()]) == 0

    jane = make_entity(JANE)
    jane_fragment = make_entity(JANE_FIRSTNAME)

    # Write entities using bulk writer
    with entities.writer() as bulk:
        bulk.add_entity(jane)

    assert len([e for e in entities.query(flush_first=True)]) == 1

    with entities.writer(origin="update") as bulk:
        bulk.add_entity(jane_fragment)

    assert len([e for e in entities.query(flush_first=True)]) == 1

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
    export_statements(dataset)

    # Add a new entity to trigger re-export
    john = make_entity(
        {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}
    )
    with entities.writer() as bulk:
        bulk.add_entity(john)
    export_statements(dataset)  # Operation's ensure_flush handles flushing

    with entities._store.open(path.EXPORTS_STATEMENTS, "r") as fh:
        reader = csv.DictReader(fh)
        data = [r for r in reader]
    assert len(data) == 6  # 2 jane (default) + 2 jane (update) + 2 john
    stmts = [Statement.from_dict(d) for d in data]
    entity_ids = set(s.entity_id for s in stmts)
    assert entity_ids == {"jane", "john"}
    origins = set(s.origin for s in stmts)
    assert origins == {"update", "default"}

    # Merge
    merge(dataset)

    # Statistics
    export_statistics(dataset)
    stats: DatasetStats = entities._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 2  # jane and john


def test_entities_export(dataset):
    """Test entity export to JSON."""
    entities = dataset.entities
    jane = make_entity(JANE)
    jane_fragment = make_entity(JANE_FIRSTNAME)

    with entities.writer() as bulk:
        bulk.add_entity(jane)
    with entities.writer(origin="update") as bulk:
        bulk.add_entity(jane_fragment)

    export_statements(dataset)  # Operation's ensure_flush handles flushing
    export_entities(dataset)

    # stream() reads from exported entities.ftm.json
    ents = [e for e in entities.stream()]
    assert len(ents) == 1
    entity = ents[0]
    assert entity.id == "jane"
    assert entity.first("name") == "Jane Doe"
    assert "update" in entity.context.get("origin")


def test_entity_multi_origin_fragments(dataset):
    """Test entity assembled from fragments with different origins.

    When the same entity ID is added from multiple origins, the resulting
    entity should contain all properties and track all origins.
    """
    entities = dataset.entities

    # Add same entity ID from three different origins with different properties
    with entities.writer(origin="source_a") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("name", "John Smith")
        entity.add("nationality", "us")
        bulk.add_entity(entity)

    with entities.writer(origin="source_b") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("birthDate", "1980-01-15")
        entity.add("gender", "male")
        bulk.add_entity(entity)

    with entities.writer(origin="source_c") as bulk:
        entity = model.make_entity("Person")
        entity.id = "multi-origin-person"
        entity.add("email", "john@example.com")
        entity.add("nationality", "gb")  # Additional nationality
        bulk.add_entity(entity)

    # Flush and export
    entities.flush()
    export_statements(dataset)
    export_entities(dataset)

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


def test_entity_multi_origin_statements(dataset):
    """Test entity assembled from individual statements with different origins.

    Add statements directly via bulk writer from multiple origins
    and verify they merge correctly.
    """
    entities = dataset.entities

    # Create statements directly for the same entity from different origins
    stmts_source_a = [
        Statement(
            entity_id="stmt-entity",
            prop="name",
            schema="Company",
            value="Acme Corporation",
            dataset=dataset.name,
        ),
        Statement(
            entity_id="stmt-entity",
            prop="jurisdiction",
            schema="Company",
            value="us",
            dataset=dataset.name,
        ),
    ]

    stmts_source_b = [
        Statement(
            entity_id="stmt-entity",
            prop="incorporationDate",
            schema="Company",
            value="2010-05-20",
            dataset=dataset.name,
        ),
        Statement(
            entity_id="stmt-entity",
            prop="status",
            schema="Company",
            value="active",
            dataset=dataset.name,
        ),
    ]

    stmts_source_c = [
        Statement(
            entity_id="stmt-entity",
            prop="website",
            schema="Company",
            value="https://acme.example.com",
            dataset=dataset.name,
        ),
    ]

    # Add statements via bulk writer with different origins
    with entities.writer(origin="registry") as bulk:
        for stmt in stmts_source_a:
            bulk.add_statement(stmt)

    with entities.writer(origin="filings") as bulk:
        for stmt in stmts_source_b:
            bulk.add_statement(stmt)

    with entities.writer(origin="enrichment") as bulk:
        for stmt in stmts_source_c:
            bulk.add_statement(stmt)

    # Flush and export
    entities.flush()
    export_statements(dataset)
    export_entities(dataset)

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
