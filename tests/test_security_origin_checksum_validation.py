"""Tests for origin / checksum / safe_name validators.

Guards the trust boundary: every caller-supplied origin or checksum is validated
before it flows into filesystem paths or parquet partition prefixes.
"""

import pytest
from followthemoney import model

from ftm_lakehouse import util
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

VALID_CHECKSUM = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"


# --- safe_name --------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "default",
        "crawl",
        "source-a",
        "source_b",
        "mapping:abc123",
        "a.b.c",
        "x",
        "A" * util.SAFE_NAME_MAX_LEN,
    ],
)
def test_safe_name_accepts_valid(value: str) -> None:
    assert util.safe_name(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "..",
        ".",
        "foo/bar",
        "foo\\bar",
        "../../etc",
        "foo..bar",  # contains ".." substring
        "foo\x00bar",
        "foo\nbar",
        "foo\tbar",
        "foo\x7fbar",
        "A" * (util.SAFE_NAME_MAX_LEN + 1),
    ],
)
def test_safe_name_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        util.safe_name(value)


def test_safe_name_rejects_non_string() -> None:
    with pytest.raises(ValueError, match="must be a string"):
        util.safe_name(123)  # type: ignore[arg-type]


def test_safe_name_field_name_in_error() -> None:
    with pytest.raises(ValueError, match="origin"):
        util.safe_name("../escape", field="origin")


# --- validate_origin --------------------------------------------------------


@pytest.mark.parametrize(
    "origin",
    [
        "default",
        "crawl",
        "mapping:bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7",
        "source-a",
        "engine_v1",
    ],
)
def test_validate_origin_accepts_valid(origin: str) -> None:
    assert util.validate_origin(origin) == origin


@pytest.mark.parametrize(
    "origin",
    [
        "../../etc/passwd",
        "foo/bar",
        "..",
        "",
    ],
)
def test_validate_origin_rejects_invalid(origin: str) -> None:
    with pytest.raises(ValueError):
        util.validate_origin(origin)


# --- validate_checksum ------------------------------------------------------


def test_validate_checksum_accepts_valid() -> None:
    assert util.validate_checksum(VALID_CHECKSUM) == VALID_CHECKSUM


@pytest.mark.parametrize(
    "checksum",
    [
        "",
        "abcde",  # too short
        "A" * 64,  # uppercase
        "g" * 64,  # non-hex char
        "../../etc/passwd" + "0" * 48,  # 64 chars but with path separators
        "0" * 63,  # one char short
        "0" * 65,  # one char over
        VALID_CHECKSUM + "0",  # too long
    ],
)
def test_validate_checksum_rejects_invalid(checksum: str) -> None:
    with pytest.raises(ValueError):
        util.validate_checksum(checksum)


def test_validate_checksum_rejects_non_string() -> None:
    with pytest.raises(ValueError):
        util.validate_checksum(None)  # type: ignore[arg-type]


# --- path-construction call sites ------------------------------------------


def test_archive_txt_rejects_traversal_origin() -> None:
    with pytest.raises(ValueError):
        path.archive_txt(VALID_CHECKSUM, "../../../etc/passwd")


def test_statement_origin_rejects_traversal() -> None:
    with pytest.raises(ValueError):
        path.statement_origin("../escape")


def test_archive_meta_rejects_traversal_file_id() -> None:
    with pytest.raises(ValueError):
        path.archive_meta(VALID_CHECKSUM, "../evil")


def test_archive_blob_rejects_invalid_checksum() -> None:
    with pytest.raises(ValueError):
        path.archive_blob("not-a-valid-checksum")


# --- EntityBuffer call sites -----------------------------------------------


def test_entity_buffer_init_rejects_traversal_origin() -> None:
    with pytest.raises(ValueError):
        EntityBuffer("test", shards=8, origin="../../evil")


def test_entity_buffer_add_entity_rejects_traversal_origin() -> None:
    buf = EntityBuffer("test", shards=8, origin="default")
    entity = model.make_entity("Person")
    entity.id = "test-person"
    entity.add("name", "Test")

    with pytest.raises(ValueError):
        buf.add_entity(entity, origin="../../evil")
