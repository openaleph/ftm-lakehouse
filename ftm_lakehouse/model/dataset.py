"""Catalog and dataset metadata models."""

from typing import TypeVar

from anystore.model import StoreModel
from anystore.types import HttpUrlStr
from ftmq.model import Catalog, Dataset

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.util import render

settings = Settings()

DM = TypeVar("DM", bound="DatasetModel")


class CatalogModel(Catalog):
    storage: StoreModel | None = None
    """Lakehouse storage base path"""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""
    public_url_prefix: HttpUrlStr | None = None
    """Public url prefix for resources"""
    shards: int = settings.entity_shards
    """Number of entity-id hash shards for the parquet store. Set once at
    dataset creation; changing it requires a full rewrite."""

    def get_public_prefix(self) -> str | None:
        if self.public_url_prefix:
            return self.public_url_prefix
        if settings.public_url_prefix:
            return render(settings.public_url_prefix, {"dataset": self.name})
