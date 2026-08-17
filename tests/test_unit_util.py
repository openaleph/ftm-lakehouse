import re

import pytest

from ftm_lakehouse import util
from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM
from ftm_lakehouse.helpers.file import make_file_id, make_folder_id
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.storage.versions import VersionStore

SHA256 = re.compile(r"^[0-9a-f]{64}$")


def test_checksum_algorithm_pin(tmp_path):
    """``CHECKSUM_ALGORITHM`` stays sha256 ("never change this") – the archive
    layout, version-snapshot dedupe tags, and file / folder entity ids all
    bake the digest shape into stored keys. Smokes every caller of anystore's
    ``make_checksum`` / ``make_data_checksum``."""
    assert CHECKSUM_ALGORITHM == "sha256"

    # repository/archive.py – content checksum of a stored file
    src = tmp_path / "src.txt"
    src.write_text("hello")
    file = ArchiveRepository("test", tmp_path / "data").store(src)
    assert SHA256.match(file.checksum)
    util.validate_checksum(file.checksum)  # the archive-layout validator agrees

    # storage/versions.py – snapshot dedupe tag carries the digest
    VersionStore(tmp_path / "data").make("config.yml", DatasetModel(name="test"))
    tag = next((tmp_path / "data").rglob("config.yml-*"))
    assert SHA256.match(tag.name.rsplit("-", 1)[1])

    # helpers/file.py – content-derived file / folder entity ids
    assert SHA256.match(make_file_id("a/path.txt", file.checksum)[len("file-") :])
    assert SHA256.match(make_folder_id("a/path")[len("folder-") :])


def test_util():
    ch = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"
    assert util.make_checksum_key(ch) == f"bb/b1/f0/{ch}"
    with pytest.raises(ValueError):
        util.make_checksum_key("abcde")

    assert util.render("{{ foo }}", {"foo": "bar"}) == "bar"


def test_util_parse_byte_size():
    assert util.parse_byte_size("1024") == 1024
    assert util.parse_byte_size("8GB") == 8 * 10**9
    assert util.parse_byte_size("8gb") == 8 * 10**9
    assert util.parse_byte_size(" 64G ") == 64 * 10**9
    assert util.parse_byte_size("512 MiB") == 512 * 2**20
    assert util.parse_byte_size("1.5GB") == 1_500_000_000
    assert util.parse_byte_size("2TiB") == 2 * 2**40
    with pytest.raises(ValueError):
        util.parse_byte_size("80%")
    with pytest.raises(ValueError):
        util.parse_byte_size("GB")
    with pytest.raises(ValueError):
        util.parse_byte_size("8 flops")
