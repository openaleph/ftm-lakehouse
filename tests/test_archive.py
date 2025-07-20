from anystore.store import get_store
from anystore.types import Uri

# from moto import mock_aws
from rigour.mime.types import PLAIN

from ftm_lakehouse.lake.archive import DatasetLakeArchive


def _test_archive(archive: DatasetLakeArchive, crawl_uri: Uri):
    crawl = get_store(crawl_uri)
    for key in crawl.iterate_keys():
        archive.archive_file(key, crawl)

    files = [f for f in archive.iter_files()]
    assert len(files) == 4

    content_hash = "5a6acf229ba576d9a40b09292595658bbb74ef56"
    assert archive.exists(content_hash)
    file = archive.lookup_file(content_hash)
    assert file.key == "utf.txt"
    assert file.checksum == content_hash
    assert file.mimetype == PLAIN
    with archive.open_file(file) as fh:
        assert fh.read() == "Îș unî©ođ€.\n".encode()

    assert b"\n".join(archive.stream_file(file)) == "Îș unî©ođ€.\n".encode()

    return True


def test_archive_local(tmp_path, fixtures_path):
    archive = DatasetLakeArchive("test", tmp_path)
    assert _test_archive(archive, fixtures_path / "src")


# @mock_aws
# def test_archive_s3_dataset(fixtures_path):
#     archive = DatasetLakeArchive("test", "s3://lakehouse/test")
#     assert _test_archive(archive, fixtures_path / "src")


# def test_archive_remote_dataset():
#     dataset = _test_archive_dataset("remote_dataset")
#     assert dataset.store.readonly
#     assert dataset.readonly
#     assert isinstance(dataset, ReadOnlyDatasetArchive)
