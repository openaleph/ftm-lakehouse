from rigour.mime.types import DEFAULT, HTML, PDF, WORD

from ftm_lakehouse.helpers import file


def test_helpers_file():
    # mime_to_schema returns a Schema object, compare names
    assert file.mime_to_schema(HTML).name == "HyperText"
    assert file.mime_to_schema(PDF).name == "Pages"
    assert file.mime_to_schema(WORD).name == "Pages"
    assert file.mime_to_schema(DEFAULT).name == "Document"
    assert file.mime_to_schema("foo").name == "Document"
