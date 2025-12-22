from rigour.mime.types import DEFAULT, HTML, PDF, WORD

from ftm_lakehouse.helpers import file


def test_helpers_file():
    assert file.mime_to_schema(HTML) == "HyperText"
    assert file.mime_to_schema(PDF) == "Pages"
    assert file.mime_to_schema(WORD) == "Pages"
    assert file.mime_to_schema(DEFAULT) == "Document"
    assert file.mime_to_schema("foo") == "Document"
