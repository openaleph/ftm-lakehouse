"""Tests for export operations - statements, entities, statistics, index."""

from anystore.io import smart_stream_csv_models
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.operation.export import (
    ExportDocumentsJob,
    ExportDocumentsOperation,
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.repository import ArchiveRepository, EntityRepository

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
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = f"tags/lakehouse/exports/statements.csv"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportStatementsJob.make(dataset=DATASET)
    op = ExportStatementsOperation(job=job, uri=tmp_path)

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
    assert (tmp_path / "exports/statements.csv").exists()


def test_operation_export_entities(tmp_path):
    """Test ExportEntitiesOperation: parquet to entities.ftm.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = f"tags/lakehouse/entities.ftm.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportEntitiesJob.make(dataset=DATASET)
    op = ExportEntitiesOperation(job=job, uri=tmp_path)

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
    assert (tmp_path / "entities.ftm.json").exists()


def test_operation_export_statistics(tmp_path):
    """Test ExportStatisticsOperation: parquet to statistics.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = "tags/lakehouse/exports/statistics.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportStatisticsJob.make(dataset=DATASET)
    op = ExportStatisticsOperation(job=job, uri=tmp_path)

    assert op.get_target() == path.EXPORTS_STATISTICS
    assert op.get_target() == "exports/statistics.json"
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
    versions = list((tmp_path / "versions").rglob("exports/statistics.json"))
    assert len(versions) >= 1


def test_operation_export_index(tmp_path):
    """Test ExportIndexOperation: generate index.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # Run prerequisites first (statistics and entities exports)
    stats_job = ExportStatisticsJob.make(dataset=DATASET)
    ExportStatisticsOperation(job=stats_job, uri=tmp_path).run()

    entities_job = ExportEntitiesJob.make(dataset=DATASET)
    ExportEntitiesOperation(job=entities_job, uri=tmp_path).run()

    # No target tag before run
    target_path = "tags/lakehouse/index.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportIndexJob.make(dataset=DATASET)
    op = ExportIndexOperation(job=job, uri=tmp_path)

    assert op.get_target() == path.INDEX
    assert op.get_target() == "index.json"
    assert op.get_dependencies() == [
        path.CONFIG,
        path.EXPORTS_STATISTICS,
        path.ENTITIES_JSON,
        path.EXPORTS_DOCUMENTS,
    ]
    assert op.get_dependencies() == [
        "config.yml",
        "exports/statistics.json",
        "entities.ftm.json",
        "exports/documents.csv",
    ]

    # Run the export operation (requires dataset model)
    dataset = DatasetModel(name=DATASET, title="Export Test Dataset")
    result = op.run(dataset=dataset)

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists (versioned, so check versions dir)
    versions = list((tmp_path / "versions").rglob("index.json"))
    assert len(versions) >= 1


def test_operation_export_documents(tmp_path, fixtures_path):
    """Test ExportDocumentsOperation: parquet to documents.csv with tags."""
    tmp_path = tmp_path / DATASET
    archive = ArchiveRepository(dataset=DATASET, uri=tmp_path)
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)

    # Archive files and write their entities
    for key in ["utf.txt", "companies.csv"]:
        doc = archive.store(fixtures_path / "src" / key)
        with repo.bulk() as writer:
            for entity in doc.make_entities():
                writer.add_entity(entity)
    repo.flush()

    # No target tag before run
    target_path = "tags/lakehouse/exports/documents.csv"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = ExportDocumentsJob.make(dataset=DATASET)
    op = ExportDocumentsOperation(job=job, uri=tmp_path)

    assert op.get_target() == path.EXPORTS_DOCUMENTS
    assert op.get_target() == "exports/documents.csv"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]

    # Run the export operation
    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Verify output file exists at hardcoded path
    assert (tmp_path / "exports/documents.csv").exists()

    # Check result
    docs = list(smart_stream_csv_models(tmp_path / path.EXPORTS_DOCUMENTS, Document))
    assert len(docs) == 2
    for doc in docs:
        assert doc.public_url.startswith(f"https://data.example.org/{DATASET}/archive/")
