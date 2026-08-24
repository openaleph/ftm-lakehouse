"""FollowTheMoney Data Lakehouse."""

from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.lake import (
    dataset_exists,
    ensure_dataset,
    get_archive,
    get_dataset_index,
    get_dataset_model,
    get_documents,
    get_entities,
    get_lakehouse,
    update_dataset,
)
from ftm_lakehouse.model.dataset import DatasetModel, set_model_class

__version__ = "0.5.0"

__all__ = [
    "Catalog",
    "DatasetModel",
    "dataset_exists",
    "ensure_dataset",
    "get_archive",
    "get_dataset_index",
    "get_dataset_model",
    "get_documents",
    "get_entities",
    "get_lakehouse",
    "set_model_class",
    "update_dataset",
]
