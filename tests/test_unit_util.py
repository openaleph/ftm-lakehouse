import pytest
from rigour.mime.types import DEFAULT, HTML, PDF, WORD

from ftm_lakehouse import util


def test_util():
    ch = "5a6acf229ba576d9a40b09292595658bbb74ef56"
    assert util.make_checksum_key(ch) == f"5a/6a/cf/{ch}"
    with pytest.raises(ValueError):
        util.make_checksum_key("abcde")

    assert util.mime_to_schema(HTML) == "HyperText"
    assert util.mime_to_schema(PDF) == "Pages"
    assert util.mime_to_schema(WORD) == "Pages"
    assert util.mime_to_schema(DEFAULT) == "Document"
    assert util.mime_to_schema("foo") == "Document"

    assert util.render("{{ foo }}", {"foo": "bar"}) == "bar"
