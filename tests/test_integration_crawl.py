from moto import mock_aws

from ftm_lakehouse.logic.crawl import crawl


@mock_aws
def test_crawl(tmp_lake, fixtures_path):
    dataset = tmp_lake.get_dataset("test1")
    res = crawl("http://localhost:8000/src", dataset)
    assert res.done == 5
    files1 = [f for f in dataset.archive.iter_files()]
    assert len(files1) == 5

    dataset = tmp_lake.get_dataset("test2")
    res = crawl(fixtures_path / "src", dataset)
    assert res.done == 5
    files2 = [f for f in dataset.archive.iter_files()]
    assert len(files2) == 5

    files1 = {f.key for f in files1}
    files2 = {f.key for f in files2}
    assert not files1 - files2, files1 - files2
    assert not files2 - files1, files2 - files1

    file = dataset.archive.lookup_file("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
    assert file.key == "testdir/test.txt"
    assert file.name == "test.txt"

    entities = list(dataset.entities.query())
    assert len(entities) == 5
    # entities = list(dataset.entities.query(origin="crawl"))
    # assert len(entities) == 4

    # dataset = tmp_lake.get_dataset("test3")
    # res = crawl("s3://data", dataset)
    # assert res.done == 4
    # files = [f for f in dataset.archive.iter_files()]
    # assert len(files) == 4


def test_crawl_globs(tmp_dataset, fixtures_path):
    res = crawl(fixtures_path / "src", tmp_dataset, exclude_glob="*.pdf")
    assert res.done == 4
    assert len([f for f in tmp_dataset.archive.iter_files()]) == 4
    res = crawl(fixtures_path / "src", tmp_dataset, glob="*.pdf")
    assert res.done == 1
    assert len([f for f in tmp_dataset.archive.iter_files()]) == 5
