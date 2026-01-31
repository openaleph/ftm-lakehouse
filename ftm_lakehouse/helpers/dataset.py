"""Dataset model helpers"""

from anystore.store.resource import UriResource
from followthemoney.dataset.resource import DataResource
from rigour.mime.types import CSV, FTM, JSON


def make_resource(
    uri: str, mime_type: str | None = None, public_url: str | None = None
) -> DataResource:
    res = UriResource(uri)
    info = res.info()
    return DataResource(
        name=res.name,
        url=public_url or uri,
        checksum=res.checksum(),
        timestamp=info.created_at,
        mime_type=mime_type or info.mimetype,
        size=info.size,
    )


def make_entities_resource(uri: str, public_url: str | None = None) -> DataResource:
    return make_resource(uri, FTM, public_url)


def make_statements_resource(uri: str, public_url: str | None = None) -> DataResource:
    return make_resource(uri, CSV, public_url)


def make_documents_resource(uri: str, public_url: str | None = None) -> DataResource:
    return make_resource(uri, CSV, public_url)


def make_statistics_resource(uri: str, public_url: str | None = None) -> DataResource:
    return make_resource(uri, JSON, public_url)
