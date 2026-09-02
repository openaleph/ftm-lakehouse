from anystore.io import smart_stream_csv_models

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.repository import (
    ArchiveRepository,
    DocumentRepository,
    EntityRepository,
)


def _archive_with_entities(archive: ArchiveRepository, entities: EntityRepository, uri):
    """Archive a file and write its entities to the entity repository."""
    file = archive.store(uri)
    with entities.writer() as writer:
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
        assert (
            doc.public_url
            == f"https://data.example.org/test/{path.archive_blob(doc.checksum)}"
        )  # pytest-env global prefix var

    # Check specific file
    utf_docs = [d for d in documents if d.name == "utf.txt"]
    assert len(utf_docs) == 1
    utf_doc = utf_docs[0]
    assert (
        utf_doc.checksum
        == "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"
    )
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
    assert path.EXPORTS_DOCUMENTS in str(repo.csv_uri())


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
    """Test incremental diff export using translog-based change detection.

    The first export writes no file - it only records the state the next diff
    is taken against. Subsequent diffs capture incremental changes via
    translog timestamps.

    Sleeps cross second boundaries because FtM truncates timestamps to seconds
    and diff detection uses first_seen >= floor(since).
    """
    import time

    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    repo = DocumentRepository("test", tmp_path)

    assert entities._statements.version is None

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    entities.flush()
    # version 0 is the empty create commit (ParquetStore._ensure_table)
    assert entities._statements.version == 1

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()
    assert entities._statements.version == 2

    # a diff reads canonical rows, so the store has to be merged first
    entities.merge()

    # First export - only records the diff state, writes no file
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 0

    # Add more data
    file3 = tmp_path / "new_file.txt"
    file3.write_text("new content")
    _archive_with_entities(archive, entities, file3)
    entities.flush()
    entities.merge()  # a diff reads canonical rows

    # Incremental diff - captures changes via translog
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 1

    # Find and verify the incremental diff contains only new_file.txt
    diff_files_sorted = sorted(diff_files, key=lambda p: p.name)
    incremental_docs = list(
        smart_stream_csv_models(diff_files_sorted[0], model=Document)
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
    entities.merge()  # a diff reads canonical rows

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()  # v1
    entities.merge()  # a diff reads canonical rows

    repo.export_csv()

    # First export - only records the diff state
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # No diff file at all - the first export writes none
    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 0


def _archive_with_origin(
    archive: ArchiveRepository, entities: EntityRepository, uri, origin: str
):
    file = archive.store(uri)
    with entities.writer(origin=origin) as writer:
        for entity in file.make_entities():
            writer.add_entity(entity)
    return file


def test_repository_document_export_csv_origin(tmp_path, fixtures_path):
    """An origin-scoped export only carries that origin's documents."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    repo = DocumentRepository("test", tmp_path)

    _archive_with_origin(
        archive, entities, fixtures_path / "src" / "utf.txt", tag.CRAWL_ORIGIN
    )
    _archive_with_origin(
        archive, entities, fixtures_path / "src" / "companies.csv", "other"
    )
    entities.flush()

    repo.export_csv()
    repo.export_csv(tag.CRAWL_ORIGIN)

    assert (tmp_path / path.EXPORTS_DOCUMENTS).exists()
    assert (tmp_path / path.export_documents(tag.CRAWL_ORIGIN)).exists()

    assert {d.name for d in repo.stream()} == {"utf.txt", "companies.csv"}
    assert {d.name for d in repo.stream(tag.CRAWL_ORIGIN)} == {"utf.txt"}

    # an origin without documents writes no file
    repo.export_csv("empty")
    assert not (tmp_path / path.export_documents("empty")).exists()


def test_repository_document_export_diff_origin(tmp_path, fixtures_path):
    """Origin-scoped diffs keep their own state and only see their origin."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    repo = DocumentRepository("test", tmp_path)

    _archive_with_origin(
        archive, entities, fixtures_path / "src" / "utf.txt", tag.CRAWL_ORIGIN
    )
    entities.flush()
    entities.merge()

    # first export only records the state both scopes diff against
    assert repo.export_diff() is not None
    assert repo.export_diff(tag.CRAWL_ORIGIN) is not None
    assert not (tmp_path / path.diffs_documents(tag.CRAWL_ORIGIN)).exists()

    # a crawled document lands in both diffs ...
    file3 = tmp_path / "new_file.txt"
    file3.write_text("new content")
    _archive_with_origin(archive, entities, file3, tag.CRAWL_ORIGIN)
    entities.flush()
    entities.merge()

    assert repo.export_diff() is not None
    assert repo.export_diff(tag.CRAWL_ORIGIN) is not None

    crawl_diffs = sorted((tmp_path / path.diffs_documents(tag.CRAWL_ORIGIN)).glob("*"))
    assert len(crawl_diffs) == 1
    docs = list(smart_stream_csv_models(crawl_diffs[0], model=Document))
    assert {d.name for d in docs} == {"new_file.txt"}

    # ... one from another origin only in the unscoped diff
    file4 = tmp_path / "other_file.txt"
    file4.write_text("other content")
    _archive_with_origin(archive, entities, file4, "other")
    entities.flush()
    entities.merge()

    assert repo.export_diff() is not None
    assert repo.export_diff(tag.CRAWL_ORIGIN) is None

    assert len(list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))) == 2
    assert len(list((tmp_path / path.diffs_documents(tag.CRAWL_ORIGIN)).glob("*"))) == 1
