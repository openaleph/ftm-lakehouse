from anystore.io import smart_stream_csv_models

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.repository import (
    ArchiveRepository,
    DocumentRepository,
    EntityRepository,
)


def _archive_with_entities(archive: ArchiveRepository, entities: EntityRepository, uri):
    """Archive a file and write its entities to the entity repository."""
    file = archive.store(uri)
    with entities.bulk() as writer:
        for entity in file.make_entities():
            writer.add_entity(entity)
    return file


def test_repository_document_collect(tmp_path, fixtures_path):
    """Test collecting documents from archived files."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    # Archive files and write their entities
    for key in ["utf.txt", "companies.csv"]:
        _archive_with_entities(archive, entities, fixtures_path / "src" / key)

    # Flush journal to parquet
    entities.flush()

    # Now collect documents from the repository
    repo = DocumentRepository("test", tmp_path)
    documents = list(repo.collect())

    assert len(documents) == 2

    # Verify document structure
    for doc in documents:
        assert doc.id
        assert doc.checksum
        assert doc.name
        assert doc.path is None  # root dir
        assert doc.size > 0
        assert doc.mimetype
        assert doc.public_url is None

    # Check specific file
    utf_docs = [d for d in documents if d.name == "utf.txt"]
    assert len(utf_docs) == 1
    utf_doc = utf_docs[0]
    assert utf_doc.checksum == "5a6acf229ba576d9a40b09292595658bbb74ef56"
    assert utf_doc.mimetype == "text/plain"


def test_repository_document_export_csv(tmp_path, fixtures_path):
    """Test exporting documents to CSV."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    # Archive files and write their entities
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()

    # Export to CSV
    repo = DocumentRepository("test", tmp_path)
    repo.export_csv()

    # Verify CSV was created
    csv_path = tmp_path / path.EXPORTS_DOCUMENTS
    assert csv_path.exists()

    # Verify CSV contents by streaming back
    streamed = list(repo.stream())
    assert len(streamed) == 2

    names = {d.name for d in streamed}
    assert "utf.txt" in names
    assert "companies.csv" in names


def test_repository_document_csv_uri(tmp_path):
    """Test csv_uri property returns correct path."""
    repo = DocumentRepository("test", tmp_path)
    assert path.EXPORTS_DOCUMENTS in str(repo.csv_uri)


def test_repository_document_empty(tmp_path):
    """Test collecting from empty repository."""
    repo = DocumentRepository("test", tmp_path)
    documents = list(repo.collect())
    assert documents == []


def test_repository_document_multi_metadata(tmp_path):
    """Test documents with same content but different paths."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    # Create files with identical content but different paths
    content = b"identical content for document test"
    file1 = tmp_path / "source1" / "doc.txt"
    file2 = tmp_path / "source2" / "same.txt"

    file1.parent.mkdir(parents=True)
    file2.parent.mkdir(parents=True)
    file1.write_bytes(content)
    file2.write_bytes(content)

    # Archive both and write entities
    result1 = _archive_with_entities(archive, entities, file1)
    result2 = _archive_with_entities(archive, entities, file2)
    entities.flush()

    # Both should produce documents
    repo = DocumentRepository("test", tmp_path)
    documents = list(repo.collect())

    assert len(documents) == 2
    assert result1.checksum == result2.checksum

    # Different IDs and names
    ids = {d.id for d in documents}
    names = {d.name for d in documents}
    assert len(ids) == 2
    assert "doc.txt" in names
    assert "same.txt" in names


def test_repository_document_export_diff(tmp_path, fixtures_path):
    """Test incremental diff export using Delta CDC.

    Initial diff copies documents.csv regardless of Delta table version.
    Subsequent diffs capture incremental changes via CDC.
    """
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    repo = DocumentRepository("test", tmp_path)

    assert entities._statements.version is None

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    entities.flush()
    assert entities._statements.version == 0

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()
    assert entities._statements.version == 1

    # Create initial diff
    diff_name_1 = repo.export_diff()
    assert diff_name_1.startswith("v1_")

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 1  # Initial diff file created

    # Verify initial diff contains both files (full export)
    initial_diff_docs = list(smart_stream_csv_models(diff_files[0], model=Document))
    assert len(initial_diff_docs) == 2
    names = {doc.name for doc in initial_diff_docs}
    assert names == {"utf.txt", "companies.csv"}

    # Add more data: creates Delta table v3
    file3 = tmp_path / "new_file.txt"
    file3.write_text("new content")
    _archive_with_entities(archive, entities, file3)
    entities.flush()

    # Incremental diff - captures changes from v2 to v3
    diff_name_2 = repo.export_diff()
    assert diff_name_2.startswith("v2_")
    assert diff_name_2 != diff_name_1

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 2

    # Find and verify the incremental diff (v2) contains only new_file.txt
    diff_files_sorted = sorted(diff_files, key=lambda p: p.name)
    incremental_docs = list(
        smart_stream_csv_models(diff_files_sorted[1], model=Document)
    )
    assert len(incremental_docs) == 1
    assert incremental_docs[0].name == "new_file.txt"


def test_repository_document_export_diff_no_changes(tmp_path, fixtures_path):
    """Test diff export when there are no new changes after initial setup."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    repo = DocumentRepository("test", tmp_path)

    # Create data and flush
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    entities.flush()  # v0

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()  # v1

    # Export documents.csv for initial diff
    repo.export_csv()

    # Initial diff (v1) - copies documents.csv
    diff_name_1 = repo.export_diff()
    assert diff_name_1.startswith("v1_")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # Only one diff file should exist (initial)
    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 1
