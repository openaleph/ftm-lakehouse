import csv

from anystore.io import smart_stream_csv_models
from anystore.io.read import smart_stream_csv
from followthemoney import Statement
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.repository import (
    ArchiveRepository,
    ArtifactsRepository,
    DocumentRepository,
    EntityRepository,
)


def _export(tmp_path, make_diff: bool = True) -> None:
    """Run the export sweep, which writes every documents origin scope."""
    job = ExportJob.make(dataset="test", make_diff=make_diff)
    ExportOperation(job=job, uri=tmp_path).run(force=True)


def _archive_with_entities(archive: ArchiveRepository, entities: EntityRepository, uri):
    """Archive a file and write its entities to the entity repository."""
    file = archive.store(uri)
    with entities.writer() as writer:
        for entity in file.make_entities():
            writer.add_entity(entity)
    return file


def test_repository_document_rows(tmp_path, fixtures_path):
    """The row one archived file contributes to the documents csv."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    # Archive files and write their entities
    for key in ["utf.txt", "companies.csv"]:
        _archive_with_entities(archive, entities, fixtures_path / "src" / key)

    # Flush journal to parquet
    entities.flush()

    _export(tmp_path)
    repo = DocumentRepository("test", tmp_path)
    documents = list(repo.stream())

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
            == f"https://data.example.org/test/{path.ArchiveKey(doc.checksum).blob}"
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
    _export(tmp_path)

    # Verify CSV was created
    csv_path = tmp_path / path.EXPORTS_DOCUMENTS
    assert csv_path.exists()

    # Verify CSV contents by streaming back
    streamed = list(repo.stream())
    assert len(streamed) == 2

    names = {d.name for d in streamed}
    assert "utf.txt" in names
    assert "companies.csv" in names


def test_repository_document_export_csv_paths(tmp_path, fixtures_path):
    """The sweep resolves folder paths from the entities it streams.

    Nothing queries the folder tree before the sweep opens – the folders come
    past as entities like any other, so the rows are staged and stamped once
    the tree is complete (`ParentsRun.finish`). A file in two folders is
    still two rows.
    """
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)
    src = fixtures_path / "src" / "utf.txt"

    with entities.writer() as writer:
        for key in ("a/b/utf.txt", "other/utf.txt"):
            for entity in archive.store(src, key=key).make_entities():
                writer.add_entity(entity)
    entities.flush()

    _export(tmp_path)

    repo = DocumentRepository("test", tmp_path)
    streamed = list(repo.stream())
    assert {d.path for d in streamed} == {"a/b", "other"}
    assert {d.relative_path for d in streamed} == {"a/b/utf.txt", "other/utf.txt"}


def test_repository_document_export_csv_document_parent(tmp_path):
    """An entity can be a csv row and path scaffolding at once.

    ``parent`` ranges over ``Folder``, which ``Email`` / ``Package`` /
    ``Workbook`` extend – so the entity a path segment comes from is often a
    document of its own, and the sweep stages it as both
    (`DocumentsArtifact.is_parent_schema`). A bare folder stays scaffolding:
    no content hash, no row.
    """
    entities = EntityRepository("test", tmp_path)
    with entities.writer() as writer:
        writer.add_entity(
            make_entity(
                {
                    "id": "mail",
                    "schema": "Email",
                    "properties": {
                        "fileName": ["inbox.eml"],
                        "contentHash": ["a" * 64],
                    },
                }
            )
        )
        writer.add_entity(
            make_entity(
                {
                    "id": "attachment",
                    "schema": "Pages",
                    "properties": {
                        "fileName": ["doc.pdf"],
                        "contentHash": ["b" * 64],
                        "parent": ["mail"],
                    },
                }
            )
        )
        writer.add_entity(
            make_entity(
                {
                    "id": "folder",
                    "schema": "Folder",
                    "properties": {"fileName": ["empty"]},
                }
            )
        )
    entities.flush()

    _export(tmp_path)

    repo = DocumentRepository("test", tmp_path)
    assert {d.name: d.path for d in repo.stream()} == {
        "inbox.eml": None,
        "doc.pdf": "inbox.eml",
    }


def test_repository_document_export_parents(tmp_path):
    """``parents.csv`` is the folder tree the documents' paths come from – every
    entity a document can sit in, with its own path."""
    entities = EntityRepository("test", tmp_path)
    with entities.writer() as writer:
        for data in (
            {
                "id": "mail",
                "schema": "Email",
                "properties": {"fileName": ["inbox.eml"]},
            },
            {"id": "root", "schema": "Folder", "properties": {"fileName": ["root"]}},
            {
                "id": "sub",
                "schema": "Folder",
                "properties": {"fileName": ["sub"], "parent": ["root"]},
            },
            {
                "id": "doc",
                "schema": "Pages",
                "properties": {
                    "fileName": ["doc.pdf"],
                    "contentHash": ["b" * 64],
                    "parent": ["sub"],
                },
            },
        ):
            writer.add_entity(make_entity(data))
    entities.flush()

    _export(tmp_path)

    with ArtifactsRepository("test", tmp_path).parents.reader("r") as fh:
        rows = list(csv.DictReader(fh))
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": "mail", "name": "inbox.eml", "path": "inbox.eml"},
        {"id": "root", "name": "root", "path": "root"},
        {"id": "sub", "name": "sub", "path": "root/sub"},
    ]
    assert {d.name: d.path for d in DocumentRepository("test", tmp_path).stream()} == {
        "doc.pdf": "root/sub"
    }


def test_repository_document_csv_uri(tmp_path):
    """Test csv_uri property returns correct path."""
    repo = DocumentRepository("test", tmp_path)
    assert str(path.EXPORTS_DOCUMENTS) in str(repo.csv_uri())


def test_repository_document_empty(tmp_path):
    """Test streaming from an empty repository."""
    repo = DocumentRepository("test", tmp_path)
    assert list(repo.stream()) == []


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
    _export(tmp_path)
    repo = DocumentRepository("test", tmp_path)
    documents = list(repo.stream())

    assert len(documents) == 2
    assert result1.checksum == result2.checksum

    # Different IDs and names
    ids = {d.id for d in documents}
    names = {d.name for d in documents}
    assert len(ids) == 2
    assert "doc.txt" in names
    assert "same.txt" in names


def test_repository_document_export_diff(tmp_path, fixtures_path, settle):
    """Test incremental diff export using translog-based change detection.

    The first export writes no file - it only records the state the next diff
    is taken against. Subsequent diffs capture incremental changes via
    translog timestamps.

    Sleeps cross second boundaries because FtM truncates timestamps to seconds
    and diff detection uses first_seen >= floor(since).
    """
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    assert entities.statements.version is None

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    entities.flush()
    # version 0 is the empty create commit (ParquetStore._ensure_table)
    assert entities.statements.version == 1

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()
    assert entities.statements.version == 2

    settle(entities)
    _export(tmp_path)

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 0

    # Add more data
    file3 = tmp_path / "new_file.txt"
    file3.write_text("new content")
    _archive_with_entities(archive, entities, file3)
    entities.flush()
    settle(entities)

    # Incremental diff - captures changes via translog
    _export(tmp_path)

    diff_files = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    assert len(diff_files) == 1

    # Find and verify the incremental diff contains only new_file.txt
    diff_files_sorted = sorted(diff_files, key=lambda p: p.name)
    incremental_docs = list(
        smart_stream_csv_models(diff_files_sorted[0], model=Document)
    )
    assert len(incremental_docs) == 1
    assert incremental_docs[0].name == "new_file.txt"


def test_repository_document_export_diff_delete(tmp_path, fixtures_path, settle):
    """A deleted document diffs as a DEL – a deleted non-document does not.

    Every diff series picks its DEL candidates out of one shared raw scan
    (`ExportSession.load_pending`), so the documents series has to do its own
    narrowing in python: schema and content hash, which a tombstoned entity
    can still be judged by. A tombstoned ``Company`` must
    not land in the documents diff – it was never a row in it.
    """
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    file = _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    with entities.writer() as writer:
        writer.add_statement(
            Statement(
                entity_id="acme",
                prop="name",
                schema="Company",
                value="Acme Inc",
                dataset="test",
            )
        )
    entities.flush()
    settle(entities)
    _export(tmp_path)
    assert not list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))

    entities.delete_entity(file.id)
    entities.delete_entity("acme")
    entities.flush()
    settle(entities)
    _export(tmp_path)

    (diff_file,) = list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))
    rows = list(smart_stream_csv(diff_file))
    assert [(r["op"], r["id"]) for r in rows] == [("DEL", file.id)]


def test_repository_document_export_diff_no_changes(tmp_path, fixtures_path, settle):
    """Test diff export when there are no new changes after initial setup."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    # Create data and flush
    _archive_with_entities(archive, entities, fixtures_path / "src" / "utf.txt")
    entities.flush()  # v0
    settle(entities)

    _archive_with_entities(archive, entities, fixtures_path / "src" / "companies.csv")
    entities.flush()  # v1
    settle(entities)

    # First export - only records the diff state
    _export(tmp_path)

    # Second export without any new data - no new diff file
    _export(tmp_path)

    # No diff file at all - the first export writes none, and the second
    # finds nothing changed
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

    # one run writes every origin scope
    _export(tmp_path)

    assert (tmp_path / path.EXPORTS_DOCUMENTS).exists()
    assert (tmp_path / path.EXPORTS_DOCUMENTS[tag.CRAWL_ORIGIN]).exists()

    assert {d.name for d in repo.stream()} == {"utf.txt", "companies.csv"}
    assert {d.name for d in repo.stream(tag.CRAWL_ORIGIN)} == {"utf.txt"}


def test_repository_document_export_diff_origin(tmp_path, fixtures_path, settle):
    """Origin-scoped diffs keep their own state and only see their origin."""
    archive = ArchiveRepository("test", tmp_path)
    entities = EntityRepository("test", tmp_path)

    _archive_with_origin(
        archive, entities, fixtures_path / "src" / "utf.txt", tag.CRAWL_ORIGIN
    )
    entities.flush()
    settle(entities)
    _export(tmp_path)
    assert not (tmp_path / path.DIFFS_DOCUMENTS[tag.CRAWL_ORIGIN]).exists()

    # a crawled document lands in both diffs ...
    file3 = tmp_path / "new_file.txt"
    file3.write_text("new content")
    _archive_with_origin(archive, entities, file3, tag.CRAWL_ORIGIN)
    entities.flush()
    settle(entities)

    _export(tmp_path)

    crawl_diffs = sorted((tmp_path / path.DIFFS_DOCUMENTS[tag.CRAWL_ORIGIN]).glob("*"))
    assert len(crawl_diffs) == 1
    docs = list(smart_stream_csv_models(crawl_diffs[0], model=Document))
    assert {d.name for d in docs} == {"new_file.txt"}

    # ... one from another origin only in the unscoped diff
    file4 = tmp_path / "other_file.txt"
    file4.write_text("other content")
    _archive_with_origin(archive, entities, file4, "other")
    entities.flush()
    settle(entities)

    _export(tmp_path)

    assert len(list((tmp_path / path.DIFFS_DOCUMENTS).glob("*.diff.csv"))) == 2
    assert len(list((tmp_path / path.DIFFS_DOCUMENTS[tag.CRAWL_ORIGIN]).glob("*"))) == 1


def test_repository_document_export_csv_multi_parent(tmp_path):
    """A file in two folders is two rows, an unresolvable parent is dropped.

    The expansion the second phase applies (`ParentsRun.finish`): one row
    per parent that resolves, and nothing at all for one that does not – as
    long as another does, else the file keeps its one unpathed row.
    """
    entities = EntityRepository("test", tmp_path)
    with entities.writer() as writer:
        for folder, name in (("folder-a", "one"), ("folder-b", "two")):
            writer.add_entity(
                make_entity(
                    {
                        "id": folder,
                        "schema": "Folder",
                        "properties": {"fileName": [name]},
                    }
                )
            )
        writer.add_entity(
            make_entity(
                {
                    "id": "doc",
                    "schema": "Pages",
                    "properties": {
                        "fileName": ["doc.pdf"],
                        "contentHash": ["a" * 64],
                        "parent": ["folder-a", "folder-b", "unknown"],
                    },
                }
            )
        )
    entities.flush()

    _export(tmp_path)

    rows = list(DocumentRepository("test", tmp_path).stream())
    assert {r.id for r in rows} == {"doc"}
    assert {r.path for r in rows} == {"one", "two"}
