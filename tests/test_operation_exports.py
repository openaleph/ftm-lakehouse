"""Tests for export operations - statements, entities, statistics, index."""

from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path, tag
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
from ftm_lakehouse.repository import EntityRepository

DATASET = "export_test"

JANE = {"id": "jane", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
JOHN = {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}


def setup_entities(repo: EntityRepository) -> None:
    """Add test entities and flush to statements store."""
    with repo.bulk(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()


def test_operation_export_statements(tmp_path):
    """Test ExportStatementsOperation: parquet to statements.csv with tags."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)
    setup_entities(repo)

    # No target tag before run
    target_path = f"{DATASET}/tags/lakehouse/exports/statements.csv"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportStatementsJob.make(dataset=DATASET)
    op = ExportStatementsOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == path.EXPORTS_STATEMENTS
    assert op.get_target() == "exports/statements.csv"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]
    assert op.get_dependencies() == ["statements/last_updated", "journal/last_updated"]

    # Run the export operation
    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists at hardcoded path
    assert (dataset_uri / "exports/statements.csv").exists()


def test_operation_export_entities(tmp_path):
    """Test ExportEntitiesOperation: parquet to entities.ftm.json with tags."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)
    setup_entities(repo)

    # No target tag before run
    target_path = f"{DATASET}/tags/lakehouse/entities.ftm.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportEntitiesJob.make(dataset=DATASET)
    op = ExportEntitiesOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == path.ENTITIES_JSON
    assert op.get_target() == "entities.ftm.json"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]
    assert op.get_dependencies() == ["statements/last_updated", "journal/last_updated"]

    # Run the export operation
    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists at hardcoded path
    assert (dataset_uri / "entities.ftm.json").exists()


def test_operation_export_statistics(tmp_path):
    """Test ExportStatisticsOperation: parquet to statistics.json with tags."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)
    setup_entities(repo)

    # No target tag before run
    target_path = f"{DATASET}/tags/lakehouse/statistics.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportStatisticsJob.make(dataset=DATASET)
    op = ExportStatisticsOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == path.STATISTICS
    assert op.get_target() == "statistics.json"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]
    assert op.get_dependencies() == ["statements/last_updated", "journal/last_updated"]

    # Run the export operation
    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists (versioned, so check versions dir)
    versions = list((dataset_uri / "versions").rglob("statistics.json"))
    assert len(versions) >= 1


def test_operation_export_index(tmp_path):
    """Test ExportIndexOperation: generate index.json with tags."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)
    setup_entities(repo)

    # Run prerequisites first (statistics and entities exports)
    stats_job = ExportStatisticsJob.make(dataset=DATASET)
    ExportStatisticsOperation(job=stats_job, lake_uri=tmp_path).run()

    entities_job = ExportEntitiesJob.make(dataset=DATASET)
    ExportEntitiesOperation(job=entities_job, lake_uri=tmp_path).run()

    # No target tag before run
    target_path = f"{DATASET}/tags/lakehouse/index.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportIndexJob.make(dataset=DATASET)
    op = ExportIndexOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == path.INDEX
    assert op.get_target() == "index.json"
    assert op.get_dependencies() == [path.STATISTICS, path.ENTITIES_JSON]
    assert op.get_dependencies() == ["statistics.json", "entities.ftm.json"]

    # Run the export operation (requires dataset model)
    from ftm_lakehouse.model.dataset import DatasetModel

    dataset = DatasetModel(name=DATASET, title="Export Test Dataset")
    result = op.run(dataset=dataset)

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists (versioned, so check versions dir)
    versions = list((dataset_uri / "versions").rglob("index.json"))
    assert len(versions) >= 1
