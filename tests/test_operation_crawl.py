from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.operation.crawl import CRAWL_ORIGIN, CrawlJob, CrawlOperation

DATASET = "carpet_crawlers"


def test_operation_crawl(fixtures_path, tmp_path):
    """Test CrawlOperation: source files to archive and entities with tags."""
    dataset_uri = tmp_path / DATASET

    # No tag before run
    assert not (dataset_uri / "tags/lakehouse/operations/crawl/last_run").exists()

    job = CrawlJob.make(dataset=DATASET, uri=fixtures_path / "src", make_entities=True)
    op = CrawlOperation(job=job, lake_uri=tmp_path)

    # Verify target and dependencies
    assert op.get_target() == tag.OP_CRAWL
    assert op.get_target() == "operations/crawl/last_run"
    assert op.get_dependencies() == []

    res = op.run()
    assert res.done == 5

    # Tag should exist at hardcoded path after run
    assert (dataset_uri / "tags/lakehouse/operations/crawl/last_run").exists()

    # Verify archived files
    files = [f for f in op.archive.iterate()]
    assert len(files) == 5

    file = op.archive.get("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
    assert file.key == "testdir/test.txt"
    assert file.name == "test.txt"

    # Verify auto-flush happened (journal should be empty, store should have data)
    assert op.entities._journal.count() == 0
    assert op.entities._statements.stats().entity_count > 0

    # Verify entities (no flush needed, CrawlOperation auto-flushes)
    entities = list(op.entities.query(origin=CRAWL_ORIGIN))
    assert len(entities) == 5 + 1  # files + folder

    assert len(list(op.entities.query(schema="Pages"))) == 1
    assert len(list(op.entities.query(schema="Folder"))) == 1


def test_operation_crawl_globs(fixtures_path, tmp_path):
    """Test CrawlOperation with glob filters."""
    job = CrawlJob.make(
        dataset=DATASET,
        uri=fixtures_path / "src",
        exclude_glob="*.pdf",
        make_entities=True,
    )
    op = CrawlOperation(job=job, lake_uri=tmp_path)
    res = op.run()
    assert res.done == 4
    entities = list(op.entities.query(origin=CRAWL_ORIGIN))
    assert len(entities) == 4 + 1  # files + folder

    job = CrawlJob.make(
        dataset=DATASET, uri=fixtures_path / "src", glob="*.pdf", make_entities=True
    )
    op = CrawlOperation(job=job, lake_uri=tmp_path)
    res = op.run()
    assert res.done == 1
    entities = list(op.entities.query(origin=CRAWL_ORIGIN))
    assert len(entities) == 5 + 1  # files + folder
