"""FollowTheMoney Data Lakehouse."""

from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import (
    ensure_dataset,
    get_archive,
    get_catalog,
    get_dataset,
    get_entities,
    get_lake,
    get_mappings,
)

__version__ = "0.1.2"

__all__ = [
    "Catalog",
    "Dataset",
    "get_catalog",
    "get_dataset",
    "get_lake",
    "ensure_dataset",
    "get_archive",
    "get_entities",
    "get_mappings",
]
