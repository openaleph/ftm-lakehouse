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
from ftmq.query import C, Query

from ftm_lakehouse.catalog import get_dataset_model, update_dataset
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.operation import ExportKind, export, make
from ftm_lakehouse.operation.crawl import crawl
from ftm_lakehouse.repository.factories import get_archive, get_entities, get_versions
from tests.conftest import (
    LAKEHOUSE_TEST_URL,
    DatasetHandle,
    docker_data_path,
    make_docker_dataset_name,
    make_test_api,
    skip_unless_docker_mode,
)

DATASET = "test"


@pytest.fixture(params=["local", "api", "docker"])
def dataset(
    request, tmp_path
) -> Generator[tuple[DatasetHandle, Path | None], None, None]:
    if request.param == "local":
        lake = get_lakehouse(tmp_path)
        yield DatasetHandle(DATASET, lake.dataset_uri(DATASET)), tmp_path / DATASET
    elif request.param == "api":
        with make_test_api(tmp_path) as base_url:
            lake = get_lakehouse(base_url)
            yield DatasetHandle(DATASET, lake.dataset_uri(DATASET)), tmp_path / DATASET
    else:
        skip_unless_docker_mode()
        name = make_docker_dataset_name()
        lake = get_lakehouse(LAKEHOUSE_TEST_URL)
        yield DatasetHandle(name, lake.dataset_uri(name)), docker_data_path(name)


def count_versions(dataset: DatasetHandle, filename: str) -> int:
    """Count how many versioned copies of a file exist."""
    return len(
        [
            v
            for v in get_entities(*dataset)._store.iterate_keys(prefix="versions")
            if v.endswith(filename)
        ]
    )


# ---------------------------------------------------------------------------
# Incremental processing
# ---------------------------------------------------------------------------


def test_e2e_workflows_initial_crawl_and_make(dataset, fixtures_path):
    """Test initial crawl followed by make generates all exports."""
    dataset, base_path = dataset
    store = get_entities(*dataset)._store

    # Initial state - nothing exists
    assert not store.exists(path.CONFIG)
    assert not store.exists(path.INDEX)
    assert not store.exists(path.EXPORTS_STATISTICS)
    assert not store.exists(path.EXPORTS_STATEMENTS)
    assert not store.exists(path.ENTITIES_JSON)

    # Crawl documents
    crawl(dataset.name, fixtures_path / "src", make_entities=True, uri=dataset.uri)

    # Run make - this should flush journal and generate all exports
    make(*dataset)

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
    crawl(dataset.name, fixtures_path / "src", make_entities=True, uri=dataset.uri)
    make(*dataset)

    # Second make - will run because dependencies were updated during first make
    # (start timestamp < dependency update timestamp)
    make(*dataset)

    # Record versions after second make
    initial_index_versions = count_versions(dataset, "index.json")
    initial_stats_versions = count_versions(dataset, "exports/statistics.json")

    # Small delay to ensure timestamps differ
    time.sleep(0.1)

    # Third make - should skip because nothing changed since second make
    make(*dataset)

    # No new versions should be created
    assert count_versions(dataset, "index.json") == initial_index_versions
    assert count_versions(dataset, "exports/statistics.json") == initial_stats_versions


def test_e2e_workflows_incremental_entity_addition(dataset):
    """Test adding entities incrementally triggers appropriate updates."""
    dataset, base_path = dataset

    # Initial entities
    with get_entities(*dataset).writer(origin="initial") as writer:
        for i in range(3):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    make(*dataset)

    initial_stats_versions = count_versions(dataset, "exports/statistics.json")

    # Verify initial state via query
    initial_entities = list(get_entities(*dataset).query())
    assert len(initial_entities) == 3

    # Add a new entity via the entities interface
    person = model.make_entity("Person")
    person.make_id("test-person-1")
    person.add("name", "John Doe")
    person.add("nationality", "us")

    get_entities(*dataset).add(person, origin="test")

    # Flush to see the new entity in queries
    get_entities(*dataset).flush()

    # Query should now return 4 entities
    all_entities = list(get_entities(*dataset).query())
    assert len(all_entities) == 4

    # Run make - should update statistics
    make(*dataset)

    # New version of statistics should exist
    assert count_versions(dataset, "exports/statistics.json") > initial_stats_versions


def test_e2e_workflows_bulk_entity_writing(dataset):
    """Test bulk entity writing with the context manager."""
    dataset, base_path = dataset

    # Create multiple entities in bulk
    with get_entities(*dataset).writer(origin="bulk_test") as writer:
        for i in range(10):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    # Flush and export
    make(*dataset)

    # Verify all entities were written
    stats: DatasetStats = get_entities(*dataset)._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 10

    # Query entities back
    entities = list(get_entities(*dataset).query(Query(C(origin="bulk_test"))))
    assert len(entities) == 10


def test_e2e_workflows_multiple_origins(dataset):
    """Test entities from multiple origins are properly tracked."""
    dataset, base_path = dataset

    # Add entities from different origins
    with get_entities(*dataset).writer(origin="source_a") as writer:
        for i in range(5):
            entity = model.make_entity("Person")
            entity.make_id(f"person-a-{i}")
            entity.add("name", f"Person A{i}")
            writer.add_entity(entity)

    with get_entities(*dataset).writer(origin="source_b") as writer:
        for i in range(3):
            entity = model.make_entity("Organization")
            entity.make_id(f"org-b-{i}")
            entity.add("name", f"Organization B{i}")
            writer.add_entity(entity)

    make(*dataset)

    # Query by origin
    source_a_entities = list(get_entities(*dataset).query(Query(C(origin="source_a"))))
    source_b_entities = list(get_entities(*dataset).query(Query(C(origin="source_b"))))

    assert len(source_a_entities) == 5
    assert len(source_b_entities) == 3

    # Total count
    stats: DatasetStats = get_entities(*dataset)._store.get(
        path.EXPORTS_STATISTICS, model=DatasetStats
    )
    assert stats.entity_count == 8


def test_e2e_workflows_export_files_created(dataset):
    """Test that exports are created after make() and grow with new data."""
    dataset, base_path = dataset
    store = get_entities(*dataset)._store

    # Add initial data
    with get_entities(*dataset).writer(origin="test") as writer:
        entity = model.make_entity("Person")
        entity.make_id("person-1")
        entity.add("name", "Initial Person")
        writer.add_entity(entity)

    make(*dataset)

    # Verify exports exist
    assert store.exists(path.EXPORTS_STATEMENTS)
    assert store.exists(path.ENTITIES_JSON)
    assert store.exists(path.EXPORTS_STATISTICS)

    # Record initial file size
    initial_csv_content = store.get(path.EXPORTS_STATEMENTS)
    initial_csv_size = len(initial_csv_content)

    # Add more data and re-export
    with get_entities(*dataset).writer(origin="test") as writer:
        entity = model.make_entity("Company")
        entity.make_id("company-1")
        entity.add("name", "New Company")
        writer.add_entity(entity)

    get_entities(*dataset).flush()
    export(dataset.name, ExportKind.statements, dataset.uri)

    # Verify the file is bigger (more statements)
    new_csv_content = store.get(path.EXPORTS_STATEMENTS)
    new_csv_size = len(new_csv_content)
    assert new_csv_size > initial_csv_size


def test_e2e_workflows_file_archive_and_entity_creation(dataset, fixtures_path):
    """Test that archived files create Document entities."""
    dataset, base_path = dataset

    # Archive a file
    file = get_archive(*dataset).store(fixtures_path / "src" / "example.pdf")

    assert file.checksum is not None
    assert file.mimetype is not None

    # Create entity from file
    entity = file.to_entity()
    get_entities(*dataset).add(entity, origin="archive")

    get_entities(*dataset).flush()

    # Query the entity back
    retrieved = get_entities(*dataset).get(entity.id)
    assert retrieved is not None
    assert retrieved.schema.name == "Pages"


def test_e2e_workflows_config_versioning(dataset):
    """Test that config changes create new versions."""
    dataset, base_path = dataset

    # Initial config - update the model
    update_dataset(dataset.name, dataset.uri, title="Initial Title")
    assert count_versions(dataset, "config.yml") == 1

    # Update config
    update_dataset(dataset.name, dataset.uri, title="Updated Title")
    assert count_versions(dataset, "config.yml") == 2

    # Update again
    update_dataset(dataset.name, dataset.uri, description="A description")
    assert count_versions(dataset, "config.yml") == 3

    # Verify current config has all updates
    current = get_dataset_model(dataset.name, dataset.uri)
    assert current.title == "Updated Title"
    assert current.description == "A description"


def test_e2e_workflows_index_includes_statistics(dataset):
    """Test that index export with stats includes entity counts."""
    dataset, base_path = dataset

    # Add some data
    with get_entities(*dataset).writer(origin="test") as writer:
        for i in range(5):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    get_entities(*dataset).flush()
    export(dataset.name, ExportKind.statements, dataset.uri)
    export(dataset.name, ExportKind.statistics, dataset.uri)

    # Make index
    export(dataset.name, ExportKind.index, dataset.uri)

    # Verify the index with statistics included
    index = get_versions(*dataset).get(path.INDEX, DatasetModel)
    assert index.stats.things.total == 5


def test_e2e_workflows_iterate_vs_query_entities(dataset):
    """Test difference between stream (from JSON) and query (from store)."""
    dataset, base_path = dataset

    # Add data
    with get_entities(*dataset).writer(origin="test") as writer:
        for i in range(3):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    assert len(list(get_entities(*dataset).stream())) == 0
    assert len(list(get_entities(*dataset).query(flush_first=True))) == 3

    # After full make, stream() also works
    make(*dataset)
    assert len(list(get_entities(*dataset).stream())) == 3


def test_e2e_workflows_get_entity_by_id(dataset):
    """Test retrieving specific entities by ID."""
    dataset, base_path = dataset

    # Add entities
    with get_entities(*dataset).writer(origin="test") as writer:
        for i in range(3):
            entity = model.make_entity("Person")
            entity.make_id(f"person-{i}")
            entity.add("name", f"Person {i}")
            writer.add_entity(entity)

    get_entities(*dataset).flush()

    # Get specific entity - note: ID format depends on make_id implementation
    entities = list(get_entities(*dataset).query())
    assert len(entities) == 3

    # Get by the actual ID
    first_entity = entities[0]
    retrieved = get_entities(*dataset).get(first_entity.id)
    assert retrieved is not None

    # Non-existent entity
    missing = get_entities(*dataset).get("non-existent-id")
    assert missing is None


def test_e2e_workflows_crawl_skip_existing(dataset, fixtures_path):
    """Test that crawl skips already existing files."""
    dataset, base_path = dataset

    # First crawl
    result1 = crawl(
        dataset.name, fixtures_path / "src", make_entities=True, uri=dataset.uri
    )
    make(*dataset)

    # Second crawl should skip existing files (archive handles deduplication)
    result2 = crawl(
        dataset.name, fixtures_path / "src", make_entities=True, uri=dataset.uri
    )

    # All files already exist → all skipped, none processed
    assert result1.done == 5
    assert result2.done == 0


def test_e2e_workflows_full_workflow_with_multiple_updates(dataset):
    """Test a realistic workflow with multiple data additions."""
    dataset, base_path = dataset

    # Phase 1: Initial entities
    with get_entities(*dataset).writer(origin="initial") as writer:
        for i in range(3):
            entity = model.make_entity("Company")
            entity.make_id(f"company-{i}")
            entity.add("name", f"Company {i}")
            writer.add_entity(entity)

    make(*dataset)

    # Verify phase 1 via query
    phase1_entities = list(get_entities(*dataset).query())
    assert len(phase1_entities) == 3

    # Phase 2: Add manual entities
    with get_entities(*dataset).writer(origin="manual") as writer:
        person = model.make_entity("Person")
        person.make_id("manual-person-1")
        person.add("name", "Manual Person")
        writer.add_entity(person)

    get_entities(*dataset).flush()

    # Verify phase 2 via query
    phase2_entities = list(get_entities(*dataset).query())
    assert len(phase2_entities) == 4

    make(*dataset)

    # Phase 3: Update config
    update_dataset(
        dataset.name,
        dataset.uri,
        title="Updated Dataset",
        description="A dataset with manual entities",
    )

    # Phase 4: Run make again - should skip since no new data
    make(*dataset)

    # Verify final state
    assert get_dataset_model(dataset.name, dataset.uri).title == "Updated Dataset"

    # Verify versioning
    assert count_versions(dataset, "config.yml") >= 1
    assert count_versions(dataset, "exports/statistics.json") >= 2


# ---------------------------------------------------------------------------
# Tag dependencies
# ---------------------------------------------------------------------------


def test_e2e_workflows_is_latest_logic(dataset):
    """Test the is_latest dependency check."""
    dataset, base_path = dataset
    tags = get_entities(*dataset)._tags

    # Add and flush data
    entity = model.make_entity("Person")
    entity.make_id("test")
    entity.add("name", "Test")
    get_entities(*dataset).add(entity, origin="test")
    get_entities(*dataset).flush()

    # Export statistics - sets the STATISTICS tag
    export(dataset.name, ExportKind.statistics, dataset.uri)

    # Statistics should now be latest relative to STATEMENTS_UPDATED
    assert tags.is_latest(path.EXPORTS_STATISTICS, [tag.STATEMENTS_UPDATED])

    # Add more data - breaks the "latest" status
    entity2 = model.make_entity("Company")
    entity2.make_id("test2")
    entity2.add("name", "Test Co")
    get_entities(*dataset).add(entity2, origin="test")
    get_entities(*dataset).flush()

    # Statistics is no longer latest
    assert not tags.is_latest(path.EXPORTS_STATISTICS, [tag.STATEMENTS_UPDATED])


# ---------------------------------------------------------------------------
# Archive operations
# ---------------------------------------------------------------------------


def test_e2e_workflows_archive_file(dataset, fixtures_path):
    """Test archiving a file."""
    dataset, base_path = dataset
    file = get_archive(*dataset).store(fixtures_path / "src" / "example.pdf")

    assert file.checksum is not None
    assert file.size > 0
    assert file.mimetype == "application/pdf"


def test_e2e_workflows_archive_lookup(dataset, fixtures_path):
    """Test looking up an archived file."""
    dataset, base_path = dataset
    file = get_archive(*dataset).store(fixtures_path / "src" / "example.pdf")

    # Lookup by checksum
    found = get_archive(*dataset).get_file(file.checksum)
    assert found.checksum == file.checksum


def test_e2e_workflows_archive_file_exists(dataset, fixtures_path):
    """Test checking if a file exists."""
    dataset, base_path = dataset
    file = get_archive(*dataset).store(fixtures_path / "src" / "example.pdf")

    assert get_archive(*dataset).exists(file.checksum)
    # Use a valid but non-existent checksum format (64 hex chars for SHA256)
    assert not get_archive(*dataset).exists("0" * 64)


def test_e2e_workflows_archive_open_file(dataset, fixtures_path):
    """Test opening an archived file."""
    dataset, base_path = dataset
    file = get_archive(*dataset).store(fixtures_path / "src" / "utf.txt")

    with get_archive(*dataset).open(file.checksum) as fh:
        content = fh.read()
        assert len(content) > 0


def test_e2e_workflows_archive_iter_files(dataset, fixtures_path):
    """Test iterating through all archived files."""
    dataset, base_path = dataset

    # Archive multiple files
    get_archive(*dataset).store(fixtures_path / "src" / "example.pdf")
    get_archive(*dataset).store(fixtures_path / "src" / "utf.txt")

    files = list(get_archive(*dataset).iterate_files())
    assert len(files) == 2
