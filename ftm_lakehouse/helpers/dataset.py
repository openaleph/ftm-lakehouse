"""Dataset model helpers"""

from pathlib import Path

from anystore.io import get_checksum, get_info
from followthemoney.dataset.resource import DataResource
from rigour.mime.types import CSV, FTM, JSON


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
