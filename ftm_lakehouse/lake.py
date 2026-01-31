"""
Public convenience functions for the lakehouse.

This module is the recommended entry point for client applications:

```python
from ftm_lakehouse import lake

# Get the lakehouse (catalog)
catalog = lake.get_lakehouse()

# Get a dataset
dataset = lake.get_dataset("my_data")

# Ensure dataset exists
dataset = lake.ensure_dataset("my_data", title="My Dataset")

# Direct repository access
entities = lake.get_entities("my_data")
archive = lake.get_archive("my_data")
mappings = lake.get_mappings("my_data")
```
"""

from typing import Any

from anystore.functools import weakref_cache as cache
from anystore.logging import get_logger
from anystore.types import Uri
from anystore.util import ensure_uri
from ftmq.model.dataset import D

from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.model import DatasetModel
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_entities,
    get_mappings,
)

log = get_logger(__name__)


@cache
def get_lakehouse(
    uri: Uri | None = None,
    model_class: type[D] = DatasetModel,
) -> Catalog[D]:
    """
    Get a lakehouse catalog.

    Args:
        uri: Storage URI (default from LAKEHOUSE_URI setting)
        model_class: Custom DatasetModel subclass

    Returns:
        Catalog instance
    """
    settings = Settings()
    storage_uri = ensure_uri(uri or settings.uri)
    log.info("Loading catalog", uri=storage_uri)
    return Catalog(uri=storage_uri, model_class=model_class)


def get_dataset(
    name: str,
    model_class: type[D] = DatasetModel,
    **data: Any,
) -> Dataset[D]:
    """
    Get a dataset by name.

    Args:
        name: Dataset name
        model_class: Custom DatasetModel subclass
        **data: Additional config data (auto-saved if dataset exists)

    Returns:
        Dataset instance
    """
    catalog = get_lakehouse(model_class=model_class)
    return catalog.get_dataset(name, **data)


def ensure_dataset(
    name: str,
    model_class: type[D] = DatasetModel,
    **data: Any,
) -> Dataset[D]:
    """
    Get a dataset and ensure it exists.

    Creates config.yml if the dataset doesn't exist.

    Args:
        name: Dataset name
        model_class: Custom DatasetModel subclass
        **data: Config data for creation

    Returns:
        Dataset instance (created if needed)
    """
    dataset = get_dataset(name, model_class=model_class, **data)
    dataset.ensure()
    return dataset


__all__ = [
    "get_archive",
    "get_entities",
    "get_mappings",
    "get_lakehouse",
    "get_dataset",
    "ensure_dataset",
]
