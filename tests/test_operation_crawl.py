from ftmq.query import C, M, Query

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.helpers.file import make_file_id
from ftm_lakehouse.operation.crawl import (
    CrawlJob,
    CrawlOperation,
    HandleExistingMode,
)

DATASET = "carpet_crawlers"
TEST_TXT_CHECKSUM = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"


def test_operation_crawl(fixtures_path, tmp_path):
    """Test CrawlOperation: source files to archive and entities with tags."""

    # No tag before run
    assert not (tmp_path / "tags/lakehouse/operations/crawl/last_run").exists()

    job = CrawlJob.make(dataset=DATASET, uri=fixtures_path / "src", make_entities=True)
    op = CrawlOperation(job=job, uri=tmp_path)

    # Verify target and dependencies
    assert op.get_target() == tag.OP_CRAWL
    assert op.get_target() == "operations/crawl/last_run"
    assert op.get_dependencies() == []

    res = op.run()
    assert res.done == 5

    # Tag should exist at hardcoded path after run
    assert (tmp_path / "tags/lakehouse/operations/crawl/last_run").exists()

    # Verify archived files
    assert len(list(op.archive.iterate_checksums())) == 5
    assert op.archive.exists(TEST_TXT_CHECKSUM)

    # Verify auto-flush happened (journal should be empty, store should have data)
    assert op.entities._journal.count() == 0
    assert op.entities.statements.num_rows > 0

    # Verify entities (no flush needed, CrawlOperation auto-flushes)
    entities = list(op.entities.query(Query(C(origin=tag.CRAWL_ORIGIN))))
    assert len(entities) == 5 + 1  # files + folder

    assert len(list(op.entities.query(Query(M(schema="Pages"))))) == 1
    (folder,) = op.entities.query(Query(M(schema="Folder")))
    assert folder.first("fileName") == "testdir"
    entity = op.entities.get(make_file_id("testdir/test.txt", TEST_TXT_CHECKSUM))
    assert entity is not None
    assert entity.first("fileName") == "test.txt"
    assert entity.first("parent") == folder.id


def test_operation_crawl_globs(fixtures_path, tmp_path):
    """Test CrawlOperation with glob filters."""
    job = CrawlJob.make(
        dataset=DATASET,
        uri=fixtures_path / "src",
        exclude_glob="*.pdf",
        make_entities=True,
    )
    op = CrawlOperation(job=job, uri=tmp_path)
    res = op.run()
    assert res.done == 4
    entities = list(op.entities.query(Query(C(origin=tag.CRAWL_ORIGIN))))
    assert len(entities) == 4 + 1  # files + folder

    job = CrawlJob.make(
        dataset=DATASET, uri=fixtures_path / "src", glob="*.pdf", make_entities=True
    )
    op = CrawlOperation(job=job, uri=tmp_path)
    res = op.run()
    assert res.done == 1
    entities = list(op.entities.query(Query(C(origin=tag.CRAWL_ORIGIN))))
    assert len(entities) == 5 + 1  # files + folder


def test_operation_crawl_existing(fixtures_path, tmp_path):
    """A re-crawl skips archived checksums only when asked to."""
    for existing, done in (
        (HandleExistingMode.overwrite, 5),
        (HandleExistingMode.overwrite, 5),
        (HandleExistingMode.skip_checksum, 0),
    ):
        job = CrawlJob.make(
            dataset=DATASET, uri=fixtures_path / "src", existing=existing
        )
        assert CrawlOperation(job=job, uri=tmp_path).run().done == done
