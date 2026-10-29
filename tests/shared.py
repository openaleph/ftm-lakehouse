"""Shared test data constants and helpers."""

from anystore.store.resource import UriResource
from anystore.types import Uri
from ftmq.types import StatementEntities

from ftm_lakehouse.helpers.file import make_file_entities
from ftm_lakehouse.repository import ArchiveRepository

JANE = {"id": "jane", "schema": "Person", "properties": {"name": ["Jane Doe"]}}
JOHN = {"id": "john", "schema": "Person", "properties": {"name": ["John Doe"]}}
BOB = {"id": "bob", "schema": "Person", "properties": {"name": ["Bob"]}}
JANE_FIRSTNAME = {
    "id": "jane",
    "schema": "Person",
    "properties": {"firstName": ["Jane"]},
}


def archive_file(
    archive: ArchiveRepository, uri: Uri, key: str | None = None
) -> StatementEntities:
    """Archive ``uri`` and make its entities, as a crawl would – ``key`` is its
    path in the crawled source (default: its file name)."""
    checksum = archive.store(uri)
    info = UriResource(uri).info()
    if key is not None:
        info = info.model_copy(update={"key": key})
    return make_file_entities(info, checksum, archive.dataset)
