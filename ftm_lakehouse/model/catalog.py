"""Catalog and dataset metadata models."""

from anystore.model import StoreModel
from ftmq.model import Catalog, Dataset


class CatalogModel(Catalog):
    storage: StoreModel | None = None
    """Lakehouse storage base path"""
    public_url_prefix: str | None = None
    """Rewrite public archive urls"""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""
