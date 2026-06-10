"""Catalog and dataset metadata models."""

from typing import TypeVar

from anystore.model import StoreModel
from anystore.types import HttpUrlStr
from ftmq.model import Catalog, Dataset

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.util import render

settings = Settings()

DM = TypeVar("DM", bound="DatasetModel")

DEFAULT_SHARDS = 0
"""Hardcoded shard-count default – a single shard. The shard count is
per-dataset configuration (``config.yml``), set once at creation; there is
deliberately no environment override so changing environments can't
mis-shard a dataset."""


class CatalogModel(Catalog):
    storage: StoreModel | None = None
    """Lakehouse storage base path"""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""
    public_url_prefix: HttpUrlStr | None = None
    """Public url prefix for resources"""
    shards: int = DEFAULT_SHARDS
    """Number of entity-id hash shards for the parquet store. ``0`` (default)
    means a single shard; huge datasets should configure ``8`` or more at
    creation for bounded per-partition working sets (e.g.
    ``ensure_dataset("big_leak", shards=8)``). Immutable after first
    write – changing it requires a full rewrite."""

    def get_public_prefix(self) -> str | None:
        if self.public_url_prefix:
            return self.public_url_prefix
        if settings.public_url_prefix:
            return render(settings.public_url_prefix, {"dataset": self.name})
