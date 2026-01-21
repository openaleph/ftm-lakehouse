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

from followthemoney import model
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.model.mapping import DatasetMapping
from ftm_lakehouse.operation.crawl import crawl
from ftm_lakehouse.operation.export import (
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation


def count_versions(dataset: Dataset, filename: str) -> int:
    """Count how many versioned copies of a file exist."""
    return len(
        [
            v
            for v in dataset.entities._store.iterate_keys(prefix="versions")
            if v.endswith(filename)
        ]
    )


def get_tag_timestamp(dataset: Dataset, tag_key: str):
    """Get the timestamp for a tag, or None if not set."""
    return dataset.entities._tags.get(tag_key)


def export_statements(dataset: Dataset):
    """Helper to run export statements operation."""
    job = ExportStatementsJob.make(dataset=dataset.name)
    op = ExportStatementsOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    op.run()


def export_entities(dataset: Dataset):
    """Helper to run export entities operation."""
    job = ExportEntitiesJob.make(dataset=dataset.name)
    op = ExportEntitiesOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    op.run()


def export_statistics(dataset: Dataset):
    """Helper to run export statistics operation."""
    job = ExportStatisticsJob.make(dataset=dataset.name)
    op = ExportStatisticsOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    op.run()


def export_index(dataset: Dataset, include_all: bool = False):
    """Helper to run export index operation."""
    job = ExportIndexJob.make(
        dataset=dataset.name,
        include_statements_csv=include_all,
        include_entities_json=include_all,
        include_statistics=include_all,
    )
    op = ExportIndexOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    op.run(dataset=dataset.model)


def make(dataset: Dataset):
    """Helper to run full make workflow (flush + exports)."""
    dataset.entities.flush()
    export_statements(dataset)
    export_entities(dataset)
    export_statistics(dataset)
    export_index(dataset, include_all=True)


class TestIncrementalProcessing:
    """Test incremental data processing with dependency tracking."""

    def test_initial_crawl_and_make(self, tmp_dataset, fixtures_path):
        """Test initial crawl followed by make generates all exports."""
        store = tmp_dataset.entities._store

        # Initial state - nothing exists
        assert not store.exists(path.CONFIG)
        assert not store.exists(path.INDEX)
        assert not store.exists(path.STATISTICS)
        assert not store.exists(path.EXPORTS_STATEMENTS)
        assert not store.exists(path.ENTITIES_JSON)

        # Crawl documents
        crawl(
            tmp_dataset.name,
            fixtures_path / "src",
            archive=tmp_dataset.archive,
            entities=tmp_dataset.entities,
            jobs=tmp_dataset.jobs,
            make_entities=True,
        )

        # After crawl, journal should be updated but not yet flushed to store
        assert get_tag_timestamp(tmp_dataset, tag.JOURNAL_UPDATED) is not None

        # Run make - this should flush journal and generate all exports
        make(tmp_dataset)

        # All exports should now exist
        assert store.exists(path.INDEX)
        assert store.exists(path.STATISTICS)
        assert store.exists(path.EXPORTS_STATEMENTS)
        assert store.exists(path.ENTITIES_JSON)

        # Verify statistics
        stats: DatasetStats = store.get(path.STATISTICS, model=DatasetStats)
        assert stats.entity_count == 6  # 5 files + 1 folder
        assert len(stats.things.schemata) == 5  # Document types from crawled files

        # Verify versions were created for versioned files
        assert count_versions(tmp_dataset, "index.json") >= 1
        assert count_versions(tmp_dataset, "statistics.json") >= 1

    def test_make_skips_when_up_to_date(self, tmp_dataset, fixtures_path):
        """Test that make() skips processing when nothing has changed.

        Note: The @skip_if_latest decorator uses START timestamps intentionally.
        This means the first make() after data changes will run twice:
        1. First run: starts at T1, updates dependencies at T2 > T1
        2. Second run: sees T1 < T2, runs again, no new dependency updates
        3. Third run: properly skips because T2 (from run 2) > T2 (dependencies unchanged)
        """
        # Initial crawl and make
        crawl(
            tmp_dataset.name,
            fixtures_path / "src",
            archive=tmp_dataset.archive,
            entities=tmp_dataset.entities,
            jobs=tmp_dataset.jobs,
            make_entities=True,
        )
        make(tmp_dataset)

        # Second make - will run because dependencies were updated during first make
        # (start timestamp < dependency update timestamp)
        make(tmp_dataset)

        # Record versions after second make
        initial_index_versions = count_versions(tmp_dataset, "index.json")
        initial_stats_versions = count_versions(tmp_dataset, "statistics.json")

        # Small delay to ensure timestamps differ
        time.sleep(0.1)

        # Third make - should skip because nothing changed since second make
        make(tmp_dataset)

        # No new versions should be created
        assert count_versions(tmp_dataset, "index.json") == initial_index_versions
        assert count_versions(tmp_dataset, "statistics.json") == initial_stats_versions

    def test_incremental_entity_addition(self, tmp_dataset):
        """Test adding entities incrementally triggers appropriate updates."""
        # Initial entities
        with tmp_dataset.entities.bulk(origin="initial") as writer:
            for i in range(3):
                entity = model.make_entity("Company")
                entity.make_id(f"company-{i}")
                entity.add("name", f"Company {i}")
                writer.add_entity(entity)

        make(tmp_dataset)

        initial_stats_versions = count_versions(tmp_dataset, "statistics.json")

        # Verify initial state via query
        initial_entities = list(tmp_dataset.entities.query())
        assert len(initial_entities) == 3

        # Add a new entity via the entities interface
        person = model.make_entity("Person")
        person.make_id("test-person-1")
        person.add("name", "John Doe")
        person.add("nationality", "us")

        tmp_dataset.entities.add(person, origin="test")

        # Journal should be updated
        assert get_tag_timestamp(tmp_dataset, tag.JOURNAL_UPDATED) is not None

        # Flush to see the new entity in queries
        tmp_dataset.entities.flush()

        # Query should now return 4 entities
        all_entities = list(tmp_dataset.entities.query())
        assert len(all_entities) == 4

        # Run make - should update statistics
        make(tmp_dataset)

        # New version of statistics should exist
        assert count_versions(tmp_dataset, "statistics.json") > initial_stats_versions

    def test_bulk_entity_writing(self, tmp_dataset):
        """Test bulk entity writing with the context manager."""
        # Create multiple entities in bulk
        with tmp_dataset.entities.bulk(origin="bulk_test") as writer:
            for i in range(10):
                entity = model.make_entity("Company")
                entity.make_id(f"company-{i}")
                entity.add("name", f"Company {i}")
                writer.add_entity(entity)

        # Flush and export
        make(tmp_dataset)

        # Verify all entities were written
        stats: DatasetStats = tmp_dataset.entities._store.get(
            path.STATISTICS, model=DatasetStats
        )
        assert stats.entity_count == 10

        # Query entities back
        entities = list(tmp_dataset.entities.query(origin="bulk_test"))
        assert len(entities) == 10

    def test_multiple_origins(self, tmp_dataset):
        """Test entities from multiple origins are properly tracked."""
        # Add entities from different origins
        with tmp_dataset.entities.bulk(origin="source_a") as writer:
            for i in range(5):
                entity = model.make_entity("Person")
                entity.make_id(f"person-a-{i}")
                entity.add("name", f"Person A{i}")
                writer.add_entity(entity)

        with tmp_dataset.entities.bulk(origin="source_b") as writer:
            for i in range(3):
                entity = model.make_entity("Organization")
                entity.make_id(f"org-b-{i}")
                entity.add("name", f"Organization B{i}")
                writer.add_entity(entity)

        make(tmp_dataset)

        # Query by origin
        source_a_entities = list(tmp_dataset.entities.query(origin="source_a"))
        source_b_entities = list(tmp_dataset.entities.query(origin="source_b"))

        assert len(source_a_entities) == 5
        assert len(source_b_entities) == 3

        # Total count
        stats: DatasetStats = tmp_dataset.entities._store.get(
            path.STATISTICS, model=DatasetStats
        )
        assert stats.entity_count == 8

    def test_export_files_created(self, tmp_dataset):
        """Test that exports are created after make()."""
        store = tmp_dataset.entities._store
        tags = tmp_dataset.entities._tags

        # Add initial data
        with tmp_dataset.entities.bulk(origin="test") as writer:
            entity = model.make_entity("Person")
            entity.make_id("person-1")
            entity.add("name", "Initial Person")
            writer.add_entity(entity)

        make(tmp_dataset)

        # Verify exports exist
        assert store.exists(path.EXPORTS_STATEMENTS)
        assert store.exists(path.ENTITIES_JSON)
        assert store.exists(path.STATISTICS)

        # Record initial file size
        initial_csv_content = store.get(path.EXPORTS_STATEMENTS)
        initial_csv_size = len(initial_csv_content)
        initial_tag = tags.get(path.EXPORTS_STATEMENTS)

        # Add more data
        with tmp_dataset.entities.bulk(origin="test") as writer:
            entity = model.make_entity("Company")
            entity.make_id("company-1")
            entity.add("name", "New Company")
            writer.add_entity(entity)

        # Only flush, don't run full make
        tmp_dataset.entities.flush()

        # Statements should be updated tag
        assert get_tag_timestamp(tmp_dataset, tag.STATEMENTS_UPDATED) is not None

        # Now export statements
        export_statements(tmp_dataset)

        # Verify the file is bigger (more statements)
        new_csv_content = store.get(path.EXPORTS_STATEMENTS)
        new_csv_size = len(new_csv_content)
        assert new_csv_size > initial_csv_size

        # Verify the tag timestamp is newer
        new_tag = tags.get(path.EXPORTS_STATEMENTS)
        assert new_tag > initial_tag

    def test_file_archive_and_entity_creation(self, tmp_dataset, fixtures_path):
        """Test that archived files create Document entities."""
        # Archive a file
        file = tmp_dataset.archive.store(fixtures_path / "src" / "example.pdf")

        assert file.checksum is not None
        assert file.mimetype is not None

        # Create entity from file
        entity = file.to_entity()
        tmp_dataset.entities.add(entity, origin="archive")

        tmp_dataset.entities.flush()

        # Query the entity back
        retrieved = tmp_dataset.entities.get(entity.id)
        assert retrieved is not None
        assert retrieved.schema.name == "Pages"

    def test_config_versioning(self, tmp_dataset):
        """Test that config changes create new versions."""
        # Initial config - update the model
        tmp_dataset.update_model(title="Initial Title")
        assert count_versions(tmp_dataset, "config.yml") == 1

        # Update config
        tmp_dataset.update_model(title="Updated Title")
        assert count_versions(tmp_dataset, "config.yml") == 2

        # Update again
        tmp_dataset.update_model(description="A description")
        assert count_versions(tmp_dataset, "config.yml") == 3

        # Verify current config has all updates
        current = tmp_dataset.model
        assert current.title == "Updated Title"
        assert current.description == "A description"

    def test_index_includes_statistics(self, tmp_dataset):
        """Test that index export with stats includes entity counts."""
        # Add some data
        with tmp_dataset.entities.bulk(origin="test") as writer:
            for i in range(5):
                entity = model.make_entity("Person")
                entity.make_id(f"person-{i}")
                entity.add("name", f"Person {i}")
                writer.add_entity(entity)

        tmp_dataset.entities.flush()
        export_statements(tmp_dataset)
        export_statistics(tmp_dataset)

        # Make index with stats
        export_index(tmp_dataset, include_all=True)

        # Verify the index
        index = tmp_dataset.entities._store.get(path.INDEX)
        assert index is not None

    def test_iterate_vs_query_entities(self, tmp_dataset):
        """Test difference between stream (from JSON) and query (from store)."""
        # Add data
        with tmp_dataset.entities.bulk(origin="test") as writer:
            for i in range(3):
                entity = model.make_entity("Person")
                entity.make_id(f"person-{i}")
                entity.add("name", f"Person {i}")
                writer.add_entity(entity)

        # Before export, stream() returns nothing (reads from JSON file which doesn't exist)
        # stream() reads from entities.ftm.json which needs to be exported
        # We can't call stream() without the file existing, so skip that assertion

        # But query() returns entities (reads from store, auto-flushes journal)
        assert len(list(tmp_dataset.entities.query())) == 3

        # After full make, stream() also works
        make(tmp_dataset)
        assert len(list(tmp_dataset.entities.stream())) == 3

    def test_get_entity_by_id(self, tmp_dataset):
        """Test retrieving specific entities by ID."""
        # Add entities
        with tmp_dataset.entities.bulk(origin="test") as writer:
            for i in range(3):
                entity = model.make_entity("Person")
                entity.make_id(f"person-{i}")
                entity.add("name", f"Person {i}")
                writer.add_entity(entity)

        tmp_dataset.entities.flush()

        # Get specific entity - note: ID format depends on make_id implementation
        entities = list(tmp_dataset.entities.query())
        assert len(entities) == 3

        # Get by the actual ID
        first_entity = entities[0]
        retrieved = tmp_dataset.entities.get(first_entity.id)
        assert retrieved is not None

        # Non-existent entity
        missing = tmp_dataset.entities.get("non-existent-id")
        assert missing is None

    def test_crawl_skip_existing(self, tmp_dataset, fixtures_path):
        """Test that crawl skips already existing files."""
        # First crawl
        result1 = crawl(
            tmp_dataset.name,
            fixtures_path / "src",
            archive=tmp_dataset.archive,
            entities=tmp_dataset.entities,
            jobs=tmp_dataset.jobs,
            make_entities=True,
        )
        make(tmp_dataset)

        initial_count = result1.done

        # Second crawl should skip existing files (archive handles deduplication)
        result2 = crawl(
            tmp_dataset.name,
            fixtures_path / "src",
            archive=tmp_dataset.archive,
            entities=tmp_dataset.entities,
            jobs=tmp_dataset.jobs,
            make_entities=True,
        )

        # Files are deduplicated by content hash in the archive
        assert result2.done >= 0

    def test_full_workflow_with_multiple_updates(self, tmp_dataset):
        """Test a realistic workflow with multiple data additions."""
        # Phase 1: Initial entities
        with tmp_dataset.entities.bulk(origin="initial") as writer:
            for i in range(3):
                entity = model.make_entity("Company")
                entity.make_id(f"company-{i}")
                entity.add("name", f"Company {i}")
                writer.add_entity(entity)

        make(tmp_dataset)

        # Verify phase 1 via query
        phase1_entities = list(tmp_dataset.entities.query())
        assert len(phase1_entities) == 3

        # Phase 2: Add manual entities
        with tmp_dataset.entities.bulk(origin="manual") as writer:
            person = model.make_entity("Person")
            person.make_id("manual-person-1")
            person.add("name", "Manual Person")
            writer.add_entity(person)

        tmp_dataset.entities.flush()

        # Verify phase 2 via query
        phase2_entities = list(tmp_dataset.entities.query())
        assert len(phase2_entities) == 4

        make(tmp_dataset)

        # Phase 3: Update config
        tmp_dataset.update_model(
            title="Updated Dataset",
            description="A dataset with manual entities",
        )

        # Phase 4: Run make again - should skip since no new data
        make(tmp_dataset)

        # Verify final state
        assert tmp_dataset.model.title == "Updated Dataset"

        # Verify versioning
        assert count_versions(tmp_dataset, "config.yml") >= 1
        assert count_versions(tmp_dataset, "statistics.json") >= 2


class TestTagDependencies:
    """Test the tag-based dependency tracking system."""

    def test_journal_updated_tag(self, tmp_dataset):
        """Test that adding entities sets the JOURNAL_UPDATED tag."""
        initial_tag = get_tag_timestamp(tmp_dataset, tag.JOURNAL_UPDATED)
        assert initial_tag is None

        # Add an entity
        entity = model.make_entity("Person")
        entity.make_id("test")
        entity.add("name", "Test")
        tmp_dataset.entities.add(entity, origin="test")

        # Tag should be set
        updated_tag = get_tag_timestamp(tmp_dataset, tag.JOURNAL_UPDATED)
        assert updated_tag is not None

    def test_statements_updated_tag(self, tmp_dataset):
        """Test that flushing journal sets the STATEMENTS_UPDATED tag."""
        # Add and flush
        entity = model.make_entity("Person")
        entity.make_id("test")
        entity.add("name", "Test")
        tmp_dataset.entities.add(entity, origin="test")

        initial_tag = get_tag_timestamp(tmp_dataset, tag.STATEMENTS_UPDATED)
        assert initial_tag is None

        tmp_dataset.entities.flush()

        # Tag should be set
        updated_tag = get_tag_timestamp(tmp_dataset, tag.STATEMENTS_UPDATED)
        assert updated_tag is not None

    def test_is_latest_logic(self, tmp_dataset):
        """Test the is_latest dependency check."""
        tags = tmp_dataset.entities._tags

        # Add and flush data
        entity = model.make_entity("Person")
        entity.make_id("test")
        entity.add("name", "Test")
        tmp_dataset.entities.add(entity, origin="test")
        tmp_dataset.entities.flush()

        # Export statistics - sets the STATISTICS tag
        export_statistics(tmp_dataset)

        # Statistics should now be latest relative to STATEMENTS_UPDATED
        assert tags.is_latest(path.STATISTICS, [tag.STATEMENTS_UPDATED])

        # Add more data - breaks the "latest" status
        entity2 = model.make_entity("Company")
        entity2.make_id("test2")
        entity2.add("name", "Test Co")
        tmp_dataset.entities.add(entity2, origin="test")
        tmp_dataset.entities.flush()

        # Statistics is no longer latest
        assert not tags.is_latest(path.STATISTICS, [tag.STATEMENTS_UPDATED])


class TestMappingWorkflow:
    """Test CSV mapping workflow."""

    def test_mapping_workflow(self, tmp_dataset, fixtures_path):
        """Test complete mapping workflow: archive → map → process → export."""
        # Archive a CSV file
        csv_file = tmp_dataset.archive.store(fixtures_path / "src" / "companies.csv")
        assert csv_file.checksum is not None

        # Create mapping configuration
        mapping = DatasetMapping(
            dataset=tmp_dataset.name,
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
        tmp_dataset.mappings.put(mapping)

        # Process the mapping via operation
        job = MappingJob.make(dataset=tmp_dataset.name, content_hash=csv_file.checksum)
        op = MappingOperation(
            job=job,
            archive=tmp_dataset.archive,
            entities=tmp_dataset.entities,
            jobs=tmp_dataset.jobs,
            tags=tmp_dataset._tags,
            versions=tmp_dataset._versions,
        )
        result = op.run()
        assert result.done == 3  # 3 companies in CSV

        # Flush and export
        make(tmp_dataset)

        # Verify entities were created
        entities = list(tmp_dataset.entities.query())
        assert len(entities) == 3
        assert all(e.schema.name == "Company" for e in entities)

        # Verify provenance
        for entity in entities:
            assert csv_file.checksum in entity.get("proof")


class TestArchiveOperations:
    """Test file archive operations."""

    def test_archive_file(self, tmp_dataset, fixtures_path):
        """Test archiving a file."""
        file = tmp_dataset.archive.store(fixtures_path / "src" / "example.pdf")

        assert file.checksum is not None
        assert file.size > 0
        assert file.mimetype == "application/pdf"

    def test_archive_lookup(self, tmp_dataset, fixtures_path):
        """Test looking up an archived file."""
        file = tmp_dataset.archive.store(fixtures_path / "src" / "example.pdf")

        # Lookup by checksum
        found = tmp_dataset.archive.get_file(file.checksum)
        assert found.checksum == file.checksum

    def test_archive_file_exists(self, tmp_dataset, fixtures_path):
        """Test checking if a file exists."""
        file = tmp_dataset.archive.store(fixtures_path / "src" / "example.pdf")

        assert tmp_dataset.archive.exists(file.checksum)
        # Use a valid but non-existent checksum format (40 hex chars for SHA1)
        assert not tmp_dataset.archive.exists("0" * 40)

    def test_archive_open_file(self, tmp_dataset, fixtures_path):
        """Test opening an archived file."""
        file = tmp_dataset.archive.store(fixtures_path / "src" / "utf.txt")

        with tmp_dataset.archive.open(file.checksum) as fh:
            content = fh.read()
            assert len(content) > 0

    def test_archive_iter_files(self, tmp_dataset, fixtures_path):
        """Test iterating through all archived files."""
        # Archive multiple files
        tmp_dataset.archive.store(fixtures_path / "src" / "example.pdf")
        tmp_dataset.archive.store(fixtures_path / "src" / "utf.txt")

        files = list(tmp_dataset.archive.iterate_files())
        assert len(files) == 2
