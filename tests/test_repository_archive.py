from anystore.store import get_store
from anystore.types import Uri
from moto import mock_aws
from rigour.mime.types import PLAIN

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.util import make_checksum_key


def _test_repository_archive(
    archive: ArchiveRepository, crawl_uri: Uri, base_path=None
):
    # Initially no archive tag
    assert not archive._tags.exists(tag.ARCHIVE_UPDATED)

    crawl = get_store(crawl_uri)
    for key in crawl.iterate_keys():
        archive.store(key, crawl)

    # Tag should be set after store operations
    assert archive._tags.exists(tag.ARCHIVE_UPDATED)
    archive_updated = archive._tags.get(tag.ARCHIVE_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/archive/last_updated").exists()

    files = [f for f in archive.iterate_files()]
    assert len(files) == 5

    content_hash = "5a6acf229ba576d9a40b09292595658bbb74ef56"
    assert archive.exists(content_hash)
    file = archive.get_file(content_hash)
    assert file.key == "utf.txt"
    assert file.checksum == content_hash
    assert file.mimetype == PLAIN
    with archive.open(file.checksum) as fh:
        assert fh.read() == "Îș unî©ođ€.\n".encode()

    assert b"\n".join(archive.stream(file.checksum)) == "Îș unî©ođ€.\n".encode()

    # Storing another file updates the tag
    archive.store("utf.txt", crawl)
    assert archive._tags.get(tag.ARCHIVE_UPDATED) > archive_updated

    return True


def test_repository_archive_local(tmp_path, fixtures_path):
    archive = ArchiveRepository("test", tmp_path)
    assert _test_repository_archive(archive, fixtures_path / "src", base_path=tmp_path)


@mock_aws
def test_repository_archive_s3_dataset(fixtures_path, moto_server):
    moto_server.create_bucket(Bucket="lakehouse")
    archive = ArchiveRepository("test", "s3://lakehouse/test")
    assert _test_repository_archive(archive, fixtures_path / "src")


# def test_repository_archive_remote_dataset():
#     dataset = _test_repository_archive_dataset("remote_dataset")
#     assert dataset.store.readonly
#     assert dataset.readonly
#     assert isinstance(dataset, ReadOnlyDatasetArchive)


def test_repository_archive_multi_metadata(tmp_path, fixtures_path):
    """Test that multiple crawlers can archive the same file with different metadata."""
    archive = ArchiveRepository("test", tmp_path)

    # Create two files with identical content but different paths
    content = b"identical content for multi-metadata test"
    file1_path = tmp_path / "crawler1" / "documents" / "file.txt"
    file2_path = tmp_path / "crawler2" / "data" / "same_file.txt"

    file1_path.parent.mkdir(parents=True)
    file2_path.parent.mkdir(parents=True)
    file1_path.write_bytes(content)
    file2_path.write_bytes(content)

    # Archive from first crawler
    result1 = archive.store(file1_path)
    checksum = result1.checksum

    # Archive same content from second crawler (different path)
    result2 = archive.store(file2_path)

    # Both should have the same checksum
    assert result1.checksum == result2.checksum

    # But different File.id (based on source path + checksum)
    assert result1.id != result2.id

    # Different source paths (key is filename, full path in raw['name'])
    assert result1.key != result2.key
    assert result1.key == "file.txt"
    assert result2.key == "same_file.txt"

    # Blob should exist only once
    blob_path = tmp_path / path.archive_blob(checksum)
    assert blob_path.exists()

    # Both metadata files should exist
    meta1_path = tmp_path / path.archive_meta(checksum, result1.id)
    meta2_path = tmp_path / path.archive_meta(checksum, result2.id)
    assert meta1_path.exists()
    assert meta2_path.exists()


def test_repository_archive_lookup_files(tmp_path):
    """Test lookup_files returns all metadata for a checksum."""
    archive = ArchiveRepository("test", tmp_path)

    content = b"content for lookup_files test"
    paths = [
        tmp_path / "source1" / "a.txt",
        tmp_path / "source2" / "b.txt",
        tmp_path / "source3" / "c.txt",
    ]

    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)

    results = [archive.store(p) for p in paths]
    checksum = results[0].checksum

    # get_files should return all 3 metadata entries
    all_files = list(archive.get_all_files(checksum))
    assert len(all_files) == 3

    # All should have the same checksum
    assert all(f.checksum == checksum for f in all_files)

    # All should have different ids
    ids = {f.id for f in all_files}
    assert len(ids) == 3


def test_repository_archive_get_file_by_id(tmp_path):
    """Test get_file with specific file_id returns correct metadata."""
    archive = ArchiveRepository("test", tmp_path)

    content = b"content for lookup by id test"
    file1 = tmp_path / "path1" / "doc.txt"
    file2 = tmp_path / "path2" / "doc.txt"

    file1.parent.mkdir(parents=True)
    file2.parent.mkdir(parents=True)
    file1.write_bytes(content)
    file2.write_bytes(content)

    result1 = archive.store(file1)
    result2 = archive.store(file2)

    # get_file without file_id returns any (first found)
    found = archive.get_file(result1.checksum)
    assert found.checksum == result1.checksum

    # get_file with specific file_id returns that specific file
    found1 = archive.get_file(result1.checksum, file_id=result1.id)
    assert found1.id == result1.id
    assert found1.key == result1.key

    found2 = archive.get_file(result1.checksum, file_id=result2.id)
    assert found2.id == result2.id
    assert found2.key == result2.key


def test_repository_archive_iter_files_multi_metadata(tmp_path):
    """Test iter_files returns all metadata entries across checksums."""
    archive = ArchiveRepository("test", tmp_path)

    # Create files with two different contents
    content_a = b"content A"
    content_b = b"content B"

    # Each content archived from 2 different paths
    paths_a = [tmp_path / "src1" / "a.txt", tmp_path / "src2" / "a_copy.txt"]
    paths_b = [tmp_path / "src1" / "b.txt", tmp_path / "src3" / "b_copy.txt"]

    for p in paths_a + paths_b:
        p.parent.mkdir(parents=True, exist_ok=True)

    for p in paths_a:
        p.write_bytes(content_a)
    for p in paths_b:
        p.write_bytes(content_b)

    for p in paths_a + paths_b:
        archive.store(p)

    # iter_files should return all 4 metadata entries
    all_files = list(archive.iterate_files())
    assert len(all_files) == 4


def test_repository_archive_put_text_multi_origin(tmp_path, fixtures_path):
    """Test put_text stores text keyed by origin."""
    archive = ArchiveRepository("test", tmp_path)

    file = archive.store(fixtures_path / "src" / "utf.txt")
    checksum = file.checksum

    # Store text from different OCR engines
    archive.put_txt(checksum, "Text from tesseract", origin="tesseract")
    archive.put_txt(checksum, "Text from azure", origin="azure")
    archive.put_txt(checksum, "Default extraction", origin="default")

    # All text files should exist
    text_tesseract = tmp_path / path.archive_txt(checksum, "tesseract")
    text_azure = tmp_path / path.archive_txt(checksum, "azure")
    text_default = tmp_path / path.archive_txt(checksum, "default")

    assert text_tesseract.exists()
    assert text_azure.exists()
    assert text_default.exists()

    assert text_tesseract.read_text() == "Text from tesseract"
    assert text_azure.read_text() == "Text from azure"
    assert text_default.read_text() == "Default extraction"


def test_repository_archive_store_and_retrieve(tmp_path, fixtures_path):
    """Test arbitrary data and blog r/w"""

    archive = ArchiveRepository("test", tmp_path)

    checksum = "5a6acf229ba576d9a40b09292595658bbb74ef56"

    # arbitrary data
    archive.put_data(checksum, "data.txt", b"hello")
    assert archive.get_data(checksum, "data.txt") == b"hello"
    assert (tmp_path / make_checksum_key(checksum) / "data.txt").exists()

    # write via file handler
    fixture = str(fixtures_path / "src/utf.txt")
    with open(fixture, "rb") as fh:
        assert archive.write_blob(fh, checksum) == checksum
    with archive.open(checksum) as fh:
        assert fh.read() == "Îș unî©ođ€.\n".encode()

    # implicit generate checksum
    fixture = str(fixtures_path / "src/utf.txt")
    with open(fixture, "rb") as fh:
        assert archive.write_blob(fh) == checksum
    with archive.open(checksum) as fh:
        assert fh.read() == "Îș unî©ođ€.\n".encode()
