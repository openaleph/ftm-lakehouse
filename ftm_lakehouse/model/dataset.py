"""Catalog and dataset metadata models."""

from typing import TypeVar

from anystore.model import StoreModel
from ftmq.model import Catalog, Dataset
from pydantic import HttpUrl


class CatalogModel(Catalog):
    storage: StoreModel | None = None
    """Lakehouse storage base path"""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""
    resources_public_url_prefix: HttpUrl | None = None
    """Public url prefix for resources"""
    archive_public_url_template: str | None = None
    """Compute public urls to source files (e.g. when using a CDN)"""


D = TypeVar("D", bound=DatasetModel)
