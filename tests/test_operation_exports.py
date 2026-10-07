"""Tests for the ExportOperation: one sweep writes every artifact.

A run writes ``statements.csv``, ``entities.ftm.json``, every
``documents.csv`` scope and ``statistics.json`` from one pass over the
entities, then ``index.json`` registering them.
"""

import hashlib
import json

import pytest
from anystore.io import smart_stream_csv_models
from ftmq.util import make_entity
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.catalog import ensure_dataset
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.file import Document
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.operation.export import settings as export_settings
from ftm_lakehouse.repository import ArchiveRepository, EntityRepository
from tests.shared import JANE, JOHN

DATASET = "export_test"


@pytest.fixture(params=(1, 3), autouse=True)
def workers(request, monkeypatch) -> int:
    """Run the whole contract suite on the serial *and* the parallel path.

    Everything an export promises has to hold either way – the artifacts, the
    freshness tags, the counts, the truncation of a stale file by an empty
    sweep. `1` is the in-process path verbatim; `3` fans the ``(shard,
    bucket)`` pairs out to worker processes and assembles their parts.
    """
    # the settings instance the operation module reads, patched directly:
    # `ftm_lakehouse.operation` exports an `export` *function* that shadows
    # the submodule, so it cannot be reached by attribute access
    monkeypatch.setattr(export_settings, "workers", request.param)
    return int(request.param)


def setup_entities(repo: EntityRepository) -> None:
    """Add test entities and flush to statements store."""
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()


def setup_documents(tmp_path, fixtures_path) -> EntityRepository:
    """Archive two files – one as a crawl would, so the origin-scoped export
    has something to pick up – and flush their entities."""
    archive = ArchiveRepository(dataset=DATASET, uri=tmp_path)
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    for key, origin in (("utf.txt", tag.CRAWL_ORIGIN), ("companies.csv", None)):
        doc = archive.store(fixtures_path / "src" / key)
        with repo.writer(origin=origin) as writer:
            for entity in doc.make_entities():
                writer.add_entity(entity)
    repo.flush()
    return repo


def make_op(tmp_path, **kwargs) -> ExportOperation:
    job = ExportJob.make(dataset=DATASET, **kwargs)
    return ExportOperation(job=job, uri=tmp_path)


def test_export_writes_every_artifact(tmp_path, fixtures_path):
    """One run, every artifact and every freshness tag."""
    tmp_path = tmp_path / DATASET
    repo = setup_documents(tmp_path, fixtures_path)

    op = make_op(tmp_path)

    # the operation keys on the content clock as a whole
    assert op.get_target() == tag.OP_EXPORT
    assert op.get_target() == "operations/export/last_run"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED]

    assert not (tmp_path / "tags/lakehouse" / path.EXPORTS_STATEMENTS).exists()

    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # every artifact, at its hardcoded path
    assert (tmp_path / "exports/statements.csv").exists()
    assert (tmp_path / "entities.ftm.json").exists()
    assert (tmp_path / "exports/documents.csv").exists()
    assert (tmp_path / "exports/documents.crawl.csv").exists()
    assert (tmp_path / "index.json").exists()
    # the two versioned ones live beside their current copy
    for key in (path.EXPORTS_STATISTICS, path.INDEX):
        assert list((tmp_path / "versions").rglob(str(key)))

    # ... and each one carries its own freshness tag, the record of when it
    # was last written
    for key in (
        path.EXPORTS_STATEMENTS,
        path.ENTITIES_JSON,
        path.EXPORTS_DOCUMENTS,
        path.EXPORTS_DOCUMENTS[tag.CRAWL_ORIGIN],
        path.EXPORTS_STATISTICS,
        path.INDEX,
    ):
        assert (tmp_path / "tags/lakehouse" / str(key)).exists(), key
        assert repo._tags.is_latest(key, [tag.STATEMENTS_UPDATED]), key

    assert make_op(tmp_path).is_fresh()


def test_export_documents_scopes(tmp_path, fixtures_path):
    """The documents csv and its crawl-scoped sibling, from the one run."""
    tmp_path = tmp_path / DATASET
    setup_documents(tmp_path, fixtures_path)

    make_op(tmp_path).run()

    docs = list(smart_stream_csv_models(tmp_path / path.EXPORTS_DOCUMENTS, Document))
    assert len(docs) == 2
    for doc in docs:
        assert doc.public_url.startswith(f"https://data.example.org/{DATASET}/archive/")

    crawl_csv = tmp_path / path.EXPORTS_DOCUMENTS[tag.CRAWL_ORIGIN]
    crawl_docs = list(smart_stream_csv_models(crawl_csv, Document))
    assert {d.name for d in crawl_docs} == {"utf.txt"}


def test_export_index_registers_the_artifacts(tmp_path, fixtures_path):
    """``index.json`` describes what the sweep wrote, mime types pinned.

    They can't drift silently: ``statements.csv`` is a CSV artifact (``FTM``
    would claim JSON), and the documents scopes are each their own resource.
    """
    tmp_path = tmp_path / DATASET
    setup_documents(tmp_path, fixtures_path)

    make_op(tmp_path).run()

    index = json.loads((tmp_path / path.INDEX).read_text())
    mimes = {
        r["name"].rsplit("/", 1)[-1]: r.get("mime_type")
        for r in index.get("resources", [])
    }
    assert mimes["statements.csv"] == CSV
    assert mimes["entities.ftm.json"] == FTM
    assert mimes["documents.csv"] == CSV
    assert mimes["documents.crawl.csv"] == CSV
    assert mimes["statistics.json"] == JSON
    # the index is the file being written, so it does not describe itself
    assert "index.json" not in mimes
    # the statistics the same sweep folded
    assert index["stats"]["entity_count"] == 2


def test_export_skips_empty_store(tmp_path):
    """Nothing to sweep still publishes the dataset's metadata."""
    tmp_path = tmp_path / DATASET
    EntityRepository(dataset=DATASET, uri=tmp_path)  # config only, no statements

    result = make_op(tmp_path).run()

    assert result.done == 1
    assert not (tmp_path / path.EXPORTS_STATEMENTS).exists()
    assert (tmp_path / path.INDEX).exists()


def test_export_index_not_rewritten_when_fresh(tmp_path):
    """A fresh run never reaches the sweep, so nothing is re-published."""
    tmp_path = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    assert make_op(tmp_path).run().done == 1
    versions = list((tmp_path / "versions").rglob(str(path.INDEX)))
    assert len(versions) == 1

    assert make_op(tmp_path).run().done == 0
    assert list((tmp_path / "versions").rglob(str(path.INDEX))) == versions


def test_export_fresh_across_merge_stale_after_write(tmp_path):
    """Exports key on the content clock, ``statements/last_updated``: a merge
    rewrites files but changes no content, so it leaves an export fresh; a
    write makes it stale and the re-run regenerates it."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)
    setup_entities(repo)  # duplicate physical rows – the read reconciles them

    def is_fresh() -> bool:
        return repo._tags.is_latest(path.EXPORTS_STATEMENTS, [tag.STATEMENTS_UPDATED])

    make_op(tmp_path).run()
    assert repo.needs_merge  # the export did not merge for itself
    assert is_fresh()

    repo.merge()
    assert not repo.needs_merge
    assert is_fresh()  # same content, other files

    setup_entities(repo)
    assert not is_fresh()

    make_op(tmp_path).run()
    assert is_fresh()


def test_export_empty_sweep_truncates_stale_artifact(tmp_path):
    """An export is a whole picture of the store, so a sweep that yields
    nothing has to leave an empty artifact – not the previous run's file,
    freshly stamped as current."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)
    make_op(tmp_path).run()

    entities = tmp_path / path.ENTITIES_JSON
    assert "jane" in entities.read_text()

    repo.delete_entity("jane")
    repo.delete_entity("john")
    repo.merge()
    make_op(tmp_path).run(force=True)

    assert entities.exists()
    assert entities.read_text() == ""


def test_export_result_counts_each_entity_once(tmp_path):
    """The session counts the sweep, the runs count their diff ops – folding a
    run total back into its own name would double the entity count."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    setup_entities(repo)

    result = make_op(tmp_path).run().result
    assert result["entities"] == 2
    assert result["statements"] == sum(1 for _ in repo.query_statements())
