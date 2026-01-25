from ftm_lakehouse.core.conventions import path
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
