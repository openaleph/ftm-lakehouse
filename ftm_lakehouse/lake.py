"""
Public convenience functions for the lakehouse.

This module is the recommended entry point for client applications –
repositories are the dataset handle:

```python
from ftm_lakehouse import ensure_dataset, get_entities, get_archive

# Get or create a dataset (config recorded at creation)
ensure_dataset("my_data", title="My Dataset", shards=8)

# Repositories per concern
entities = get_entities("my_data")
archive = get_archive("my_data")

# Multi-dataset concerns go through the catalog
from ftm_lakehouse import get_lakehouse

for name in get_lakehouse().list_datasets():
    print(name)
```
"""

from functools import lru_cache

from anystore.logging import get_logger
from anystore.types import Uri
from anystore.util import ensure_uri, mask_uri

from ftm_lakehouse.catalog import (
    Catalog,
    dataset_exists,
    ensure_dataset,
    get_dataset_index,
    get_dataset_model,
    update_dataset,
)
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.repository.factories import (
    LRU_MAX,
    get_archive,
    get_documents,
    get_entities,
)

log = get_logger(__name__)


@lru_cache(maxsize=LRU_MAX)
def get_lakehouse(uri: Uri | None = None) -> Catalog:
    """
    Get a lakehouse catalog.

    Args:
        uri: Storage URI (default from LAKEHOUSE_URI setting)

    Returns:
        Catalog instance
    """
    settings = Settings()
    storage_uri = ensure_uri(uri or settings.uri)
    log.info("Loading catalog", uri=mask_uri(storage_uri))
    return Catalog(uri=storage_uri)


__all__ = [
    "Catalog",
    "dataset_exists",
    "ensure_dataset",
    "get_archive",
    "get_dataset_index",
    "get_dataset_model",
    "get_documents",
    "get_entities",
    "get_lakehouse",
    "update_dataset",
]
