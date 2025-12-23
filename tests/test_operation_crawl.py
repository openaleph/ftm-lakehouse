from ftm_lakehouse.operation.crawl import CRAWL_ORIGIN, CrawlJob, CrawlOperation, crawl

DATASET = "carpet_crawlers"


def test_operation_crawl(fixtures_path, tmp_path):
    job = CrawlJob.make(dataset=DATASET, uri=fixtures_path / "src", make_entities=True)
    op = CrawlOperation(job=job, lake_uri=tmp_path)
    res = op.run()
    assert res.done == 5

    files = [f for f in op.archive.iterate()]
    assert len(files) == 5

    files = {f.key for f in files}

    file = op.archive.get("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
    assert file.key == "testdir/test.txt"
    assert file.name == "test.txt"

    entities = list(op.entities.query(origin=CRAWL_ORIGIN))
    assert len(entities) == 5 + 1  # fiels + folder

    # assert len(list(op.entities.query(schema__not="Pages"))) == 5  # FIXME
    assert len(list(op.entities.query(schema="Pages"))) == 1
    assert len(list(op.entities.query(schema="Folder"))) == 1


def test_operation_crawl_globs(fixtures_path, tmp_path):
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
    assert len(entities) == 4 + 1  # fiels + folder
    job = CrawlJob.make(
        dataset=DATASET, uri=fixtures_path / "src", glob="*.pdf", make_entities=True
    )
    op = CrawlOperation(job=job, lake_uri=tmp_path)
    res = op.run()
    assert res.done == 1
    entities = list(op.entities.query(origin=CRAWL_ORIGIN))
    assert len(entities) == 5 + 1  # fiels + folder


def test_operation_crawl_function(fixtures_path, tmp_path):
    """Test the crawl() convenience function."""
    result = crawl(
        dataset=DATASET,
        uri=fixtures_path / "src",
        lake_uri=tmp_path,
        make_entities=True,
    )
    assert result.done == 5
    assert result.running is False
    assert result.stopped is not None
