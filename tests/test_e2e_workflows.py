"""
Comprehensive tests for incremental data processing workflow.

Tests the dependency tracking and skip logic for:
- Journal writes and flushes
- Statement store updates
- Export generation (statements.csv, entities.ftm.json, statistics.json)
- Index generation
- Versioning of generated files
"""

import time
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import model
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.api.main import (
    archive_router,
    entities_router,
    journal_router,
    operations_router,
)
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.model.mapping import DatasetMapping
from ftm_lakehouse.operation import (
    export_index,
    export_statements,
    export_statistics,
    make,
)
from ftm_lakehouse.operation.crawl import crawl
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from tests.conftest import (
    LAKEHOUSE_TEST_URL,
    docker_data_path,
    make_docker_dataset_name,
    make_test_api,
    skip_unless_docker_mode,
)

DATASET = "test"


@pytest.fixture(params=["local", "api", "docker"])
def dataset(request, tmp_path) -> Generator[tuple[Dataset, Path | None], None, None]:
    if request.param == "local":
        lake = get_lakehouse(tmp_path)
        yield lake.get_dataset(DATASET), tmp_path / DATASET
    elif request.param == "api":
        routers = [entities_router, journal_router, operations_router, archive_router]
        with make_test_api(tmp_path, routers) as base_url:
            lake = get_lakehouse(base_url)
            yield lake.get_dataset(DATASET), tmp_path / DATASET
    else:
        skip_unless_docker_mode()
        name = make_docker_dataset_name()
        lake = get_lakehouse(LAKEHOUSE_TEST_URL)
        yield lake.get_dataset(name), docker_data_path(name)


def count_versions(dataset: Dataset, filename: str) -> int:
    """Count how many versioned copies of a file exist."""
    return len(
        [
            v
            for v in dataset.entities._store.iterate_keys(prefix="versions")
            if v.endswith(filename)
        ]
    )


# ---------------------------------------------------------------------------
# Incremental processing
# ---------------------------------------------------------------------------


def test_e2e_workflows_initial_crawl_and_make(dataset, fixtures_path):
    """Test initial crawl followed by make generates all exports."""
    dataset, base_path = dataset
    store = dataset.entities._store

    # Initial state - nothing exists
    assert not store.exists(path.CONFIG)
    assert not store.exists(path.INDEX)
    assert not store.exists(path.EXPORTS_STATISTICS)
    assert not store.exists(path.EXPORTS_STATEMENTS)
    assert not store.exists(path.ENTITIES_JSON)

    # Crawl documents
    crawl(dataset, fixtures_path / "src", make_entities=True)

    # Run make - this should flush journal and generate all exports
    make(dataset)

    # All exports should now exist
    assert store.exists(path.INDEX)
    assert store.exists(path.EXPORTS_STATISTICS)
    assert store.exists(path.EXPORTS_STATEMENTS)
    assert store.exists(path.ENTITIES_JSON)

    # Verify statistics
    stats: DatasetStats = store.get(path.EXPORTS_STATISTICS, model=DatasetStats)
    assert stats.entity_count == 6  # 5 files + 1 folder
    assert len(stats.things.schemata) == 5  # Document types from crawled files

    # Verify versions were created for versioned files
    assert count_versions(dataset, "index.json") >= 1
    assert count_versions(dataset, "exports/statistics.json") >= 1


def test_e2e_workflows_make_skips_when_up_to_date(dataset, fixtures_path, request):
    """Test that make() skips processing when nothing has changed.

    Note: The freshness checks use START timestamps intentionally.
    This means the first make() after data changes will run twice:
    1. First run: starts at T1, updates dependencies at T2 > T1
    2. Second run: sees T1 < T2, runs again, no new dependency updates
    3. Third run: properly skips because T2 (from run 2) > T2 (dependencies unchanged)
    """
    if "docker" in request.node.name:
        pytest.skip(
            "freshness ordering across nginx + UDS round-trips is too loose "
            "for the 100ms timing window this test uses"
        )
    dataset, _ = dataset

    # Initial crawl and make
    crawl(dataset, fixtures_path / "src", make_entities=True)
    make(dataset)

    # Second make - will run because dependencies were updated during first make
    # (start timestamp < dependency update timestamp)
    make(dataset)

    # Record versions after second make
    initial_index_versions = count_versions(dataset, "index.json")
    initial_stats_versions = count_versions(dataset, "exports/statistics.json")

    # Small delay to ensure timestamps differ
    time.sleep(0.1)

    # Third make - should skip because nothing changed since second make
    make(dataset)

    # No new versions should be created
    assert count_versions(dataset, "index.json") == initial_index_versions
    assert count_versions(dataset, "exports/statistics.json") == initial_stats_versions


def test_e2e_workflows_incremental_entity_addition(dataset):
    """Test adding entities incrementally triggers appropriate updates."""
    dataset, base_path = dataset

    # Initial entities
    with dataset.entities.writer(origin="initial") as writer:
        for i in range(3):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    make(dataset)

    initial_stats_versions = count_versions(dataset, "exports/statistics.json")

    # Verify initial state via query
    initial_entities = list(dataset.entities.query())
    assert len(initial_entities) == 3

    # Add a new entity via the entities interface
    person = model.make_entity("Person")
    person.make_id("test-person-1")
    person.add("name", "John Doe")
    person.add("nationality", "us")

    dataset.entities.add(person, origin="test")

    # Flush to see the new entity in queries
    dataset.entities.flush()

    # Query should now return 4 entities
    all_entities = list(dataset.entities.query())
    assert len(all_entities) == 4

    # Run make - should update statistics
    make(dataset)

    # New version of statistics should exist
    assert count_versions(dataset, "exports/statistics.json") > initial_stats_versions


def test_e2e_workflows_bulk_entity_writing(dataset):
    """Test bulk entity writing with the context manager."""
    dataset, base_path = dataset

    # Create multiple entities in bulk
    with dataset.entities.writer(origin="bulk_test") as writer:
        for i in range(10):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    # Flush and export
    make(dataset)

    # Verify all entities were written
    stats: DatasetStats = dataset.entities._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 10

    # Query entities back
    entities = list(dataset.entities.query(origin="bulk_test"))
    assert len(entities) == 10


def test_e2e_workflows_multiple_origins(dataset):
    """Test entities from multiple origins are properly tracked."""
    dataset, base_path = dataset

    # Add entities from different origins
    with dataset.entities.writer(origin="source_a") as writer:
        for i in range(5):
            entity = model.make_entity("Person")
            entity.make_id(f"person-a-{i}")
            entity.add("name", f"Person A{i}")
            writer.add_entity(entity)

    with dataset.entities.writer(origin="source_b") as writer:
        for i in range(3):
            entity = model.make_entity("Organization")
            entity.make_id(f"org-b-{i}")
            entity.add("name", f"Organization B{i}")
            writer.add_entity(entity)

    make(dataset)

    # Query by origin
    source_a_entities = list(dataset.entities.query(origin="source_a"))
    source_b_entities = list(dataset.entities.query(origin="source_b"))

    assert len(source_a_entities) == 5
    assert len(source_b_entities) == 3

    # Total count
    stats: DatasetStats = dataset.entities._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 8


def test_e2e_workflows_export_files_created(dataset):
    """Test that exports are created after make() and grow with new data."""
    dataset, base_path = dataset
    store = dataset.entities._store

    # Add initial data
    with dataset.entities.writer(origin="test") as writer:
        entity = model.make_entity("Person")
        entity.make_id("person-1")
        entity.add("name", "Initial Person")
        writer.add_entity(entity)

    make(dataset)

    # Verify exports exist
    assert store.exists(path.EXPORTS_STATEMENTS)
    assert store.exists(path.ENTITIES_JSON)
    assert store.exists(path.EXPORTS_STATISTICS)

    # Record initial file size
    initial_csv_content = store.get(path.EXPORTS_STATEMENTS)
    initial_csv_size = len(initial_csv_content)

    # Add more data and re-export
    with dataset.entities.writer(origin="test") as writer:
        entity = model.make_entity("Company")
        entity.make_id("company-1")
        entity.add("name", "New Company")
        writer.add_entity(entity)

    dataset.entities.flush()
    export_statements(dataset)

    # Verify the file is bigger (more statements)
    new_csv_content = store.get(path.EXPORTS_STATEMENTS)
    new_csv_size = len(new_csv_content)
    assert new_csv_size > initial_csv_size


def test_e2e_workflows_file_archive_and_entity_creation(dataset, fixtures_path):
    """Test that archived files create Document entities."""
    dataset, base_path = dataset

    # Archive a file
    file = dataset.archive.store(fixtures_path / "src" / "example.pdf")

    assert file.checksum is not None
    assert file.mimetype is not None

    # Create entity from file
    entity = file.to_entity()
    dataset.entities.add(entity, origin="archive")

    dataset.entities.flush()

    # Query the entity back
    retrieved = dataset.entities.get(entity.id)
    assert retrieved is not None
    assert retrieved.schema.name == "Pages"


def test_e2e_workflows_config_versioning(dataset):
    """Test that config changes create new versions."""
    dataset, base_path = dataset

    # Initial config - update the model
    dataset.update_model(title="Initial Title")
    assert count_versions(dataset, "config.yml") == 1

    # Update config
    dataset.update_model(title="Updated Title")
    assert count_versions(dataset, "config.yml") == 2

    # Update again
    dataset.update_model(description="A description")
    assert count_versions(dataset, "config.yml") == 3

    # Verify current config has all updates
    current = dataset.model
    assert current.title == "Updated Title"
    assert current.description == "A description"


def test_e2e_workflows_index_includes_statistics(dataset):
    """Test that index export with stats includes entity counts."""
    dataset, base_path = dataset

    # Add some data
    with dataset.entities.writer(origin="test") as writer:
        for i in range(5):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    dataset.entities.flush()
    export_statements(dataset)
    export_statistics(dataset)

    # Make index
    export_index(dataset)

    # Verify the index with statistics included
    index = dataset._versions.get(path.INDEX, DatasetModel)
    assert index.stats.things.total == 5


def test_e2e_workflows_iterate_vs_query_entities(dataset):
    """Test difference between stream (from JSON) and query (from store)."""
    dataset, base_path = dataset

    # Add data
    with dataset.entities.writer(origin="test") as writer:
        for i in range(3):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    assert len(list(dataset.entities.stream())) == 0
    assert len(list(dataset.entities.query(flush_first=True))) == 3

    # After full make, stream() also works
    make(dataset)
    assert len(list(dataset.entities.stream())) == 3


def test_e2e_workflows_get_entity_by_id(dataset):
    """Test retrieving specific entities by ID."""
    dataset, base_path = dataset

    # Add entities
    with dataset.entities.writer(origin="test") as writer:
        for i in range(3):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    dataset.entities.flush()

    # Get specific entity - note: ID format depends on make_id implementation
    entities = list(dataset.entities.query())
    assert len(entities) == 3

    # Get by the actual ID
    first_entity = entities[0]
    retrieved = dataset.entities.get(first_entity.id)
    assert retrieved is not None

    # Non-existent entity
    missing = dataset.entities.get("non-existent-id")
    assert missing is None


def test_e2e_workflows_crawl_skip_existing(dataset, fixtures_path):
    """Test that crawl skips already existing files."""
    dataset, base_path = dataset

    # First crawl
    result1 = crawl(dataset, fixtures_path / "src", make_entities=True)
    make(dataset)

    # Second crawl should skip existing files (archive handles deduplication)
    result2 = crawl(dataset, fixtures_path / "src", make_entities=True)

    # All files already exist → all skipped, none processed
    assert result1.done == 5
    assert result2.done == 0


def test_e2e_workflows_full_workflow_with_multiple_updates(dataset):
    """Test a realistic workflow with multiple data additions."""
    dataset, base_path = dataset

    # Phase 1: Initial entities
    with dataset.entities.writer(origin="initial") as writer:
        for i in range(3):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    make(dataset)

    # Verify phase 1 via query
    phase1_entities = list(dataset.entities.query())
    assert len(phase1_entities) == 3

    # Phase 2: Add manual entities
    with dataset.entities.writer(origin="manual") as writer:
        person = model.make_entity("Person")
        person.make_id("manual-person-1")
        person.add("name", "Manual Person")
        writer.add_entity(person)

    dataset.entities.flush()

    # Verify phase 2 via query
    phase2_entities = list(dataset.entities.query())
    assert len(phase2_entities) == 4

    make(dataset)

    # Phase 3: Update config
    dataset.update_model(
        title="Updated Dataset",
        description="A dataset with manual entities",
    )

    # Phase 4: Run make again - should skip since no new data
    make(dataset)

    # Verify final state
    assert dataset.model.title == "Updated Dataset"

    # Verify versioning
    assert count_versions(dataset, "config.yml") >= 1
    assert count_versions(dataset, "exports/statistics.json") >= 2


# ---------------------------------------------------------------------------
# Tag dependencies
# ---------------------------------------------------------------------------


def test_e2e_workflows_is_latest_logic(dataset):
    """Test the is_latest dependency check."""
    dataset, base_path = dataset
    tags = dataset.entities._tags

    # Add and flush data
    entity = model.make_entity("Person")
    entity.make_id("test")
    entity.add("name", "Test")
    dataset.entities.add(entity, origin="test")
    dataset.entities.flush()

    # Export statistics - sets the STATISTICS tag
    export_statistics(dataset)

    # Statistics should now be latest relative to STATEMENTS_UPDATED
    assert tags.is_latest(path.EXPORTS_STATISTICS, [tag.STATEMENTS_UPDATED])

    # Add more data - breaks the "latest" status
    entity2 = model.make_entity("Company")
    entity2.make_id("test2")
    entity2.add("name", "Test Co")
    dataset.entities.add(entity2, origin="test")
    dataset.entities.flush()

    # Statistics is no longer latest
    assert not tags.is_latest(path.EXPORTS_STATISTICS, [tag.STATEMENTS_UPDATED])


# ---------------------------------------------------------------------------
# Mapping workflow
# ---------------------------------------------------------------------------


def test_e2e_workflows_mapping(dataset, fixtures_path):
    """Test complete mapping workflow: archive -> map -> process -> export."""
    dataset, base_path = dataset

    # Archive a CSV file
    csv_file = dataset.archive.store(fixtures_path / "src" / "companies.csv")
    assert csv_file.checksum is not None

    # Create mapping configuration
    mapping = DatasetMapping(
        dataset=dataset.name,
        content_hash=csv_file.checksum,
        queries=[
            {
                "entities": {
                    "company": {
                        "schema": "Company",
                        "keys": ["id"],
                        "properties": {
                            "name": {"column": "name"},
                            "jurisdiction": {"column": "jurisdiction"},
                        },
                    }
                }
            }
        ],
    )
    dataset.mappings.put(mapping)

    # Process the mapping via operation
    job = MappingJob.make(dataset=dataset.name, content_hash=csv_file.checksum)
    op = MappingOperation(
        job=job,
        archive=dataset.archive,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset._tags,
        versions=dataset._versions,
    )
    result = op.run()
    assert result.done == 3  # 3 companies in CSV

    # Flush and export
    make(dataset)

    # Verify entities were created
    entities = list(dataset.entities.query())
    assert len(entities) == 3
    assert all(e.schema.name == "Company" for e in entities)

    # Verify provenance
    for entity in entities:
        assert csv_file.checksum in entity.get("proof")


# ---------------------------------------------------------------------------
# Archive operations
# ---------------------------------------------------------------------------


def test_e2e_workflows_archive_file(dataset, fixtures_path):
    """Test archiving a file."""
    dataset, base_path = dataset
    file = dataset.archive.store(fixtures_path / "src" / "example.pdf")

    assert file.checksum is not None
    assert file.size > 0
    assert file.mimetype == "application/pdf"


def test_e2e_workflows_archive_lookup(dataset, fixtures_path):
    """Test looking up an archived file."""
    dataset, base_path = dataset
    file = dataset.archive.store(fixtures_path / "src" / "example.pdf")

    # Lookup by checksum
    found = dataset.archive.get_file(file.checksum)
    assert found.checksum == file.checksum


def test_e2e_workflows_archive_file_exists(dataset, fixtures_path):
    """Test checking if a file exists."""
    dataset, base_path = dataset
    file = dataset.archive.store(fixtures_path / "src" / "example.pdf")

    assert dataset.archive.exists(file.checksum)
    # Use a valid but non-existent checksum format (64 hex chars for SHA256)
    assert not dataset.archive.exists("0" * 64)


def test_e2e_workflows_archive_open_file(dataset, fixtures_path):
    """Test opening an archived file."""
    dataset, base_path = dataset
    file = dataset.archive.store(fixtures_path / "src" / "utf.txt")

    with dataset.archive.open(file.checksum) as fh:
        content = fh.read()
        assert len(content) > 0


def test_e2e_workflows_archive_iter_files(dataset, fixtures_path):
    """Test iterating through all archived files."""
    dataset, base_path = dataset

    # Archive multiple files
    dataset.archive.store(fixtures_path / "src" / "example.pdf")
    dataset.archive.store(fixtures_path / "src" / "utf.txt")

    files = list(dataset.archive.iterate_files())
    assert len(files) == 2
