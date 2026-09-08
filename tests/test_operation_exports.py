"""Tests for the ExportOperation kinds - statements, entities, statistics, documents, index."""

import json

from anystore.io import smart_stream_csv_models
from ftmq.util import make_entity
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
from ftm_lakehouse.repository import ArchiveRepository, EntityRepository
from tests.shared import JANE, JOHN

DATASET = "export_test"


def setup_entities(repo: EntityRepository) -> None:
    """Add test entities and flush to statements store."""
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()


def make_op(kind: ExportKind, tmp_path, **kwargs) -> ExportOperation:
    job = ExportJob.make(dataset=DATASET, kind=kind, **kwargs)
    return ExportOperation(job=job, uri=tmp_path)


def test_operation_export_statements(tmp_path):
    """Export kind=statements: parquet to statements.csv with tags."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = "tags/lakehouse/exports/statements.csv"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    op = make_op(ExportKind.statements, tmp_path)

    assert op.get_target() == path.EXPORTS_STATEMENTS
    assert op.get_target() == "exports/statements.csv"
    # exports reflect canonical content, so they go stale against the merge
    # clock - not against raw appends or the journal, which `prepare()` has
    # already resolved by the time the freshness check runs
    assert op.get_dependencies() == [tag.STATEMENTS_OPTIMIZED]
    assert op.get_dependencies() == ["statements/last_optimized"]

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
    """Export kind=entities: parquet to entities.ftm.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = "tags/lakehouse/entities.ftm.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    op = make_op(ExportKind.entities, tmp_path)

    assert op.get_target() == path.ENTITIES_JSON
    assert op.get_target() == "entities.ftm.json"
    assert op.get_dependencies() == [tag.STATEMENTS_OPTIMIZED]

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
    """Export kind=statistics: parquet to statistics.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # No target tag before run
    target_path = "tags/lakehouse/exports/statistics.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    op = make_op(ExportKind.statistics, tmp_path)

    assert op.get_target() == path.EXPORTS_STATISTICS
    assert op.get_target() == "exports/statistics.json"
    assert op.get_dependencies() == [tag.STATEMENTS_OPTIMIZED]

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
    """Export kind=index: generate index.json with tags."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    # Run prerequisites first (statistics and entities exports)
    make_op(ExportKind.statistics, tmp_path).run()
    make_op(ExportKind.entities, tmp_path).run()

    # No target tag before run
    target_path = "tags/lakehouse/index.json"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    op = make_op(ExportKind.index, tmp_path)

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

    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()

    # Resource mime types are pinned so they can't drift silently –
    # statements.csv is a CSV artifact (FTM_STMT would claim JSON).
    make_op(ExportKind.statements, tmp_path).run()
    make_op(ExportKind.index, tmp_path).run(force=True)
    index = json.loads((tmp_path / path.INDEX).read_text())
    mimes = {
        r["name"].rsplit("/", 1)[-1]: r.get("mime_type")
        for r in index.get("resources", [])
    }
    assert mimes["statements.csv"] == CSV
    assert mimes["entities.ftm.json"] == FTM
    assert mimes["statistics.json"] == JSON
    assert "documents.csv" not in mimes  # no documents export in this test

    # Verify output file exists (versioned, so check versions dir)
    versions = list((tmp_path / "versions").rglob("index.json"))
    assert len(versions) >= 1


def test_export_sweep_writes_every_artifact_once(tmp_path):
    """`ExportKind.all` produces the streamed artifacts from a single pass."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    op = make_op(ExportKind.all, tmp_path)
    assert op.get_target() == tag.OP_EXPORT
    assert op.get_target() == "operations/export/last_run"
    assert op.get_dependencies() == [tag.STATEMENTS_OPTIMIZED]

    result = op.run()
    assert result.done == 1

    assert (tmp_path / path.EXPORTS_STATEMENTS).exists()
    assert (tmp_path / path.ENTITIES_JSON).exists()
    # every artifact the sweep wrote carries its own freshness tag, so a
    # single-kind export afterwards sees itself up to date
    assert (tmp_path / "tags/lakehouse" / path.EXPORTS_STATEMENTS).exists()
    assert (tmp_path / "tags/lakehouse" / path.ENTITIES_JSON).exists()
    assert repo._tags.is_latest(path.ENTITIES_JSON, [tag.STATEMENTS_OPTIMIZED])
    assert make_op(ExportKind.entities, tmp_path).is_fresh()


def test_export_stale_after_optimize(tmp_path):
    """A merge that rewrote partitions stamps STATEMENTS_OPTIMIZED, so exports
    taken before it go stale and re-run instead of skipping as 'up-to-date'
    with duplicate / undeleted rows baked in."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)
    setup_entities(repo)  # duplicate physical rows -> merge will rewrite

    def is_fresh() -> bool:
        return repo._tags.is_latest(path.EXPORTS_STATEMENTS, [tag.STATEMENTS_OPTIMIZED])

    make_op(ExportKind.statements, tmp_path).run()
    # the run prepared itself: the duplicates were merged *before* the CSV was
    # written, so it is canonical and fresh straight away
    assert not repo.needs_merge
    assert is_fresh()

    # a merge with nothing to rewrite does not dirty anything
    repo.merge()
    assert is_fresh()

    # new duplicate rows, then a merge that rewrites them: the CSV predates
    # the canonical content it claims to hold
    setup_entities(repo)
    repo.merge()
    assert not is_fresh()

    # a re-run regenerates instead of skipping, and is fresh once more
    make_op(ExportKind.statements, tmp_path).run()
    assert is_fresh()


def test_operation_export_documents(tmp_path, fixtures_path):
    """Export kind=documents: parquet to documents.csv with tags."""
    tmp_path = tmp_path / DATASET
    archive = ArchiveRepository(dataset=DATASET, uri=tmp_path)
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)

    # Archive files and write their entities – one of them as a crawl would,
    # so the origin-scoped export has something to pick up
    for key, origin in (("utf.txt", tag.CRAWL_ORIGIN), ("companies.csv", None)):
        doc = archive.store(fixtures_path / "src" / key)
        with repo.writer(origin=origin) as writer:
            for entity in doc.make_entities():
                writer.add_entity(entity)
    repo.flush()

    # No target tag before run
    target_path = "tags/lakehouse/exports/documents.csv"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    op = make_op(ExportKind.documents, tmp_path)

    assert op.get_target() == path.EXPORTS_DOCUMENTS
    assert op.get_target() == "exports/documents.csv"
    assert op.get_dependencies() == [tag.STATEMENTS_OPTIMIZED]

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

    # ... and the same run wrote the crawl-scoped csv next to it
    crawl_csv = tmp_path / path.EXPORTS_DOCUMENTS[tag.CRAWL_ORIGIN]
    assert crawl_csv.exists()
    crawl_docs = list(smart_stream_csv_models(crawl_csv, Document))
    assert {d.name for d in crawl_docs} == {"utf.txt"}


def test_export_empty_sweep_truncates_stale_artifact(tmp_path):
    """An export is a whole picture of the store, so a sweep that yields
    nothing has to leave an empty artifact – not the previous run's file,
    freshly stamped as current."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)
    make_op(ExportKind.all, tmp_path).run()

    entities = tmp_path / path.ENTITIES_JSON
    assert "jane" in entities.read_text()

    repo.delete_entity("jane")
    repo.delete_entity("john")
    repo.merge()
    make_op(ExportKind.all, tmp_path).run(force=True)

    assert entities.exists()
    assert entities.read_text() == ""


def test_export_result_counts_each_entity_once(tmp_path):
    """The session counts the sweep, the runs count their diff ops – folding a
    run total back into its own name would double the entity count."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    result = make_op(ExportKind.all, tmp_path).run().result
    assert result["entities"] == 2
    assert result["statements"] == sum(1 for _ in repo.query_statements())
