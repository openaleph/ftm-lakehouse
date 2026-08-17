import csv
import itertools
from collections import Counter
from typing import Generator

import pytest
from followthemoney import Statement, model
from ftmq.model.stats import DatasetStats
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.logic.compress import CompressKind, decompress_stream
from ftm_lakehouse.operation import ExportKind, export, optimize
from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import (
    LAKEHOUSE_TEST_URL,
    make_docker_dataset_name,
    make_test_api,
    skip_unless_docker_mode,
)
from tests.shared import JANE, JANE_FIRSTNAME


@pytest.fixture(
    params=list(
        itertools.product(
            ["local", "api", "docker"], [None, CompressKind.gz, CompressKind.zst]
        )
    )
)
def dataset(request, tmp_path) -> Generator[DatasetRef, None, None]:
    backend, compression = request.param
    if backend == "local":
        lake = get_lakehouse(tmp_path)
        lake.ensure_dataset("test", compression=compression)
        yield DatasetRef("test", lake.dataset_uri("test"))
    elif backend == "api":
        with make_test_api(tmp_path) as base_url:
            lake = get_lakehouse(base_url)
            lake.ensure_dataset("test", compression=compression)
            yield DatasetRef("test", lake.dataset_uri("test"))
    else:
        # docker: real nginx fronting the lakehouse Granian UDS. Unique
        # dataset name keeps concurrent / repeated runs isolated.
        skip_unless_docker_mode()
        name = make_docker_dataset_name()
        lake = get_lakehouse(LAKEHOUSE_TEST_URL)
        lake.ensure_dataset(name, compression=compression)
        yield DatasetRef(name, lake.dataset_uri(name))


def test_entities(dataset):
    """Test the unified DatasetEntities interface."""
    entities = get_entities(dataset.name, dataset.uri)

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
    assert set(jane.to_dict()["origin"]) == {"default", "update"}

    # Export statements.csv
    export(dataset.name, ExportKind.statements, dataset.uri)

    # Add a new entity to trigger re-export
    john = make_entity(
        {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}
    )
    with entities.writer() as bulk:
        bulk.add_entity(john)
    export(
        dataset.name, ExportKind.statements, dataset.uri
    )  # Operation's ensure_flush handles flushing

    with (
        entities._store.open(entities.EXPORTS_STATEMENTS) as fh,
        decompress_stream(fh, entities.compression, "r") as out,
    ):
        reader = csv.DictReader(out)
        data = [r for r in reader]
    assert len(data) == 6  # 2 jane (default) + 2 jane (update) + 2 john
    stmts = [Statement.from_dict(d) for d in data]
    entity_ids = dict(Counter(s.entity_id for s in stmts))
    assert entity_ids == {"jane": 4, "john": 2}
    origins = dict(Counter(s.origin for s in stmts))
    assert origins == {"default": 4, "update": 2}

    # Merge
    optimize(dataset.name, dataset.uri)

    # Statistics
    export(dataset.name, ExportKind.statistics, dataset.uri)
    stats: DatasetStats = entities._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 2  # jane and john


def test_entities_export(dataset):
    """Test entity export to JSON."""
    entities = get_entities(dataset.name, dataset.uri)
    jane = make_entity(JANE)
    jane_fragment = make_entity(JANE_FIRSTNAME)

    with entities.writer() as bulk:
        bulk.add_entity(jane)
    with entities.writer(origin="update") as bulk:
        bulk.add_entity(jane_fragment)

    export(
        dataset.name, ExportKind.statements, dataset.uri
    )  # Operation's ensure_flush handles flushing
    export(dataset.name, ExportKind.entities, dataset.uri)

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
    entities = get_entities(dataset.name, dataset.uri)

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
    export(dataset.name, ExportKind.statements, dataset.uri)
    export(dataset.name, ExportKind.entities, dataset.uri)

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


def test_entity_multi_origin_statements(dataset):
    """Test entity assembled from individual statements with different origins.

    Add statements directly via bulk writer from multiple origins
    and verify they merge correctly.
    """
    entities = get_entities(dataset.name, dataset.uri)

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
    export(dataset.name, ExportKind.statements, dataset.uri)
    export(dataset.name, ExportKind.entities, dataset.uri)

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
    with (
        entities._store.open(entities.EXPORTS_STATEMENTS) as fh,
        decompress_stream(fh, entities.compression, "r") as out,
    ):
        reader = csv.DictReader(out)
        rows = [r for r in reader]

    stmt_origins = dict(Counter(r["origin"] for r in rows))
    assert stmt_origins == {"registry": 2, "filings": 2, "enrichment": 1}
    assert len(rows) == 5  # 2 + 2 + 1 statements
