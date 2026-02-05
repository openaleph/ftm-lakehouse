"""Tests for download archive operation."""

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import (
    ExportDocumentsJob,
    ExportDocumentsOperation,
)
from ftm_lakehouse.repository import ArchiveRepository, EntityRepository

DATASET = "download_test"


def test_operation_download_archive(tmp_path, fixtures_path):
    """Test DownloadArchiveOperation: archive files exported to target with nice paths."""
    dataset_path = tmp_path / DATASET
    archive = ArchiveRepository(dataset=DATASET, uri=dataset_path)
    repo = EntityRepository(dataset=DATASET, uri=dataset_path)

    # Archive files and write their entities
    for key in ["utf.txt", "companies.csv"]:
        doc = archive.store(fixtures_path / "src" / key)
        with repo.bulk() as writer:
            for entity in doc.make_entities():
                writer.add_entity(entity)
    repo.flush()

    # Run the documents export first (it's a dependency)
    docs_job = ExportDocumentsJob.make(dataset=DATASET)
    ExportDocumentsOperation(job=docs_job, uri=dataset_path).run()
    assert (dataset_path / path.EXPORTS_DOCUMENTS).exists()

    # Create target directory
    target_path = tmp_path / "download_target"
    target_path.mkdir()

    # Verify target/dependencies on the operation
    job = DownloadArchiveJob.make(dataset=DATASET, target=str(target_path))
    op = DownloadArchiveOperation(job=job, uri=dataset_path)

    assert op.get_target() == tag.OP_DOWNLOAD_ARCHIVE
    assert op.get_dependencies() == [path.EXPORTS_DOCUMENTS]

    # Run the download
    result = op.run()

    assert result.done == 2
    assert result.running is False
    assert result.stopped is not None

    # Verify files were downloaded to target with their original names
    downloaded = list(target_path.rglob("*"))
    downloaded_files = [p for p in downloaded if p.is_file()]
    assert len(downloaded_files) == 2

    names = {p.name for p in downloaded_files}
    assert "utf.txt" in names
    assert "companies.csv" in names
