from pathlib import Path
from typing import Generator

import pytest
from anystore.store import get_store

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.util import make_checksum_key
from tests.conftest import (
    LAKEHOUSE_TEST_URL,
    docker_data_path,
    make_docker_dataset_name,
    make_test_api,
    skip_unless_docker_mode,
)


@pytest.fixture(params=["local", "api", "s3", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[ArchiveRepository, Path | None], None, None]:
    if request.param == "local":
        yield ArchiveRepository("test", tmp_path), tmp_path
    elif request.param == "api":
        with make_test_api(tmp_path) as base_url:
            yield ArchiveRepository("test", f"{base_url}/test"), tmp_path / "test"
    elif request.param == "s3":
        moto_server = request.getfixturevalue("moto_server")
        bucket = moto_server.create_bucket(Bucket="lakehouse")
        yield ArchiveRepository("test", "s3://lakehouse/test"), None
        bucket.objects.all().delete()  # the server keeps its state across tests
    else:  # docker
        skip_unless_docker_mode()
        name = make_docker_dataset_name()
        yield ArchiveRepository(name, f"{LAKEHOUSE_TEST_URL}/{name}"), docker_data_path(
            name
        )


def test_repository_archive(repo, fixtures_path):
    archive, base_path = repo
    crawl_uri = fixtures_path / "src"

    # Initially no archive tag
    assert not archive._tags.exists(tag.ARCHIVE_UPDATED)

    crawl = get_store(crawl_uri)
    for key in crawl.iterate_keys():
        archive.store(crawl.to_uri(key))

    # Tag should be set after store operations
    assert archive._tags.exists(tag.ARCHIVE_UPDATED)
    archive_updated = archive._tags.get(tag.ARCHIVE_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/archive/last_updated").exists()

    checksums = list(archive.iterate_checksums())
    assert len(checksums) == 5

    content_hash = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"
    assert content_hash in checksums
    if base_path:
        assert (base_path / "archive" / "bb/b1/f0" / content_hash / "blob").exists()
    assert archive.exists(content_hash)
    with archive.open(content_hash) as fh:
        assert fh.read() == "Îș unî©ođ€.\n".encode()

    assert b"\n".join(archive.stream(content_hash)) == "Îș unî©ođ€.\n".encode()

    # Storing another file updates the tag
    assert archive.store(crawl.to_uri("utf.txt")) == content_hash
    assert archive._tags.get(tag.ARCHIVE_UPDATED) > archive_updated

    return True


def test_repository_archive_same_content(repo, tmp_path):
    """Identical content archived from different paths is one blob."""
    archive, base_path = repo

    content = b"identical content from two paths"
    file1_path = tmp_path / "crawler1" / "documents" / "file.txt"
    file2_path = tmp_path / "crawler2" / "data" / "same_file.txt"

    file1_path.parent.mkdir(parents=True, exist_ok=True)
    file2_path.parent.mkdir(parents=True, exist_ok=True)
    file1_path.write_bytes(content)
    file2_path.write_bytes(content)

    checksum = archive.store(file1_path)
    assert archive.store(file2_path) == checksum
    assert list(archive.iterate_checksums()) == [checksum]

    if base_path:
        blob_dir = base_path / path.ArchiveKey(checksum)
        assert [p.name for p in blob_dir.iterdir()] == [path.ARCHIVE_BLOB]


def test_repository_archive_iterate_checksums(repo, tmp_path):
    """Every blob once, and only blobs – raw data named `blob` is not one."""
    archive, _ = repo

    paths = {
        b"content A": [tmp_path / "src1" / "a.txt", tmp_path / "src2" / "a_copy.txt"],
        b"content B": [tmp_path / "src1" / "b.txt", tmp_path / "src3" / "b_copy.txt"],
    }
    checksums = set()
    for content, files in paths.items():
        for p in files:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(content)
            checksums.add(archive.store(p))
    assert len(checksums) == 2

    archive.put_data(next(iter(checksums)), "pages/blob", b"not a blob")
    assert set(archive.iterate_checksums()) == checksums


def test_repository_archive_put_text_multi_origin(repo, fixtures_path):
    """Test put_text stores text keyed by origin."""
    archive, base_path = repo

    checksum = archive.store(fixtures_path / "src" / "utf.txt")

    # Store text from different OCR engines
    archive.put_txt(checksum, "Text from tesseract", origin="tesseract")
    archive.put_txt(checksum, "Text from azure", origin="azure")
    archive.put_txt(checksum, "Default extraction", origin="default")

    # Verify storage layout on local backends
    if base_path:
        text_tesseract = base_path / path.ArchiveKey(checksum).txt("tesseract")
        text_azure = base_path / path.ArchiveKey(checksum).txt("azure")
        text_default = base_path / path.ArchiveKey(checksum).txt("default")

        assert text_tesseract.exists()
        assert text_azure.exists()
        assert text_default.exists()

        assert text_tesseract.read_text() == "Text from tesseract"
        assert text_azure.read_text() == "Text from azure"
        assert text_default.read_text() == "Default extraction"


def test_repository_archive_store_and_retrieve(repo, fixtures_path):
    """Test arbitrary data and blob r/w"""
    archive, base_path = repo

    checksum = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"

    # arbitrary data
    archive.put_data(checksum, "pages/data.txt", b"hello")
    assert archive.get_data(checksum, "pages/data.txt") == b"hello"
    assert archive.get_txt(checksum) is None
    if base_path:
        assert (base_path / path.ArchiveKey(checksum) / "pages/data.txt").exists()
        assert not (base_path / make_checksum_key(checksum)).exists()

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
