"""Dataset model helpers"""

from functools import cache
from pathlib import Path

from anystore.io import get_checksum, get_info
from anystore.types import Uri
from anystore.util import join_uri
from followthemoney.dataset.resource import DataResource
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.core.settings import Settings


def make_resource(uri: str, mime_type: str | None = None) -> DataResource:
    info = get_info(uri)
    path = Path(uri)
    return DataResource(
        name=path.name,
        url=uri,
        checksum=get_checksum(uri),
        timestamp=info.created_at,
        mime_type=mime_type or info.mimetype,
        size=info.size,
    )


def make_entities_resource(uri: str) -> DataResource:
    return make_resource(uri, FTM)


def make_statements_resource(uri: str) -> DataResource:
    return make_resource(uri, CSV)


def make_statistics_resource(uri: str) -> DataResource:
    return make_resource(uri, JSON)


@cache
def make_dataset_uri(name: str, base_uri: Uri | None = None) -> str:
    if base_uri is None:
        settings = Settings()
        base_uri = settings.uri
    return join_uri(base_uri, name)
