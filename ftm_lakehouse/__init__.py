"""FollowTheMoney Data Lakehouse."""

from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import (
    ensure_dataset,
    get_archive,
    get_dataset,
    get_entities,
    get_lakehouse,
    get_mappings,
)

__version__ = "0.2.0"

__all__ = [
    "Catalog",
    "Dataset",
    "get_lakehouse",
    "get_dataset",
    "ensure_dataset",
    "get_archive",
    "get_entities",
    "get_mappings",
]
