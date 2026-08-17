"""Dataset config lifecycle + the slim multi-dataset catalog.

Module functions own the ``config.yml`` lifecycle – existence, fresh model
reads, merge-writes and get-or-create. Day-to-day data access goes through
the repository factories (``get_entities("name")``); :class:`Catalog` covers
the remaining multi-dataset concerns: enumerating what exists under one
storage root and resolving per-dataset uris (the API server keeps one as
``app.state.lake``).

Config freshness contract: repositories snapshot their
:class:`~ftm_lakehouse.model.dataset.DatasetModel` at construction and are
LRU-cached, so :func:`update_dataset` invalidates the factory caches after
every write – repositories fetched *afterwards* see the new config, while
instances held across the write keep their old snapshot. Config that affects
the storage layout (``shards``, ``compression``) must be set at creation
(``ensure_dataset("big_leak", shards=8)``) and is immutable after the first
write.
"""

from functools import cached_property
from pathlib import Path
from typing import Any, Generator

from anystore.logging import get_logger
from anystore.store import Store, get_store
from anystore.types import Uri
from anystore.util import join_uri, mask_uri

from ftm_lakehouse.core.api import ensure_api_uri
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.dataset import DatasetModel, get_model_class
from ftm_lakehouse.repository import factories
from ftm_lakehouse.repository.base import DatasetHandle, dataset_uri

log = get_logger(__name__)


def _dataset_store(name: str, uri: Uri | None = None) -> Store:
    return get_store(ensure_api_uri(dataset_uri(name, uri)), serialization_mode="raw")


def _load_model(store: Store, name: str, **data: Any) -> DatasetModel:
    """Merge ``config.yml`` with ``data`` into the registered model class."""
    data["name"] = name
    data.pop("storage", None)
    return get_model_class()(**load_config(store, **data))


def dataset_exists(name: str, uri: Uri | None = None) -> bool:
    """Whether the dataset exists (has a ``config.yml``)."""
    return _dataset_store(name, uri).exists(path.CONFIG)


def get_dataset_model(name: str, uri: Uri | None = None) -> DatasetModel:
    """The dataset's config, read fresh from ``config.yml`` on every call.

    Args:
        name: Dataset name.
        uri: Dataset storage root override (default:
            ``{LAKEHOUSE_URI}/{name}``).
    """
    return _load_model(_dataset_store(name, uri), name)


def get_dataset_index(name: str, uri: Uri | None = None) -> DatasetModel:
    """The dataset's published ``index.json``, falling back to the config.

    The index is the config enriched with export resources and statistics,
    written by the ``index`` export operation.
    """
    versions = factories.get_versions(name, uri)
    index = versions.get(path.INDEX, model=get_model_class(), raise_on_nonexist=False)
    if index is not None:
        return index
    return get_dataset_model(name, uri)


def update_dataset(name: str, uri: Uri | None = None, **data: Any) -> DatasetModel:
    """Merge ``data`` into the dataset's ``config.yml`` (versioned snapshot).

    Invalidates the repository factory caches afterwards so newly fetched
    repositories see the fresh config; instances held across the write keep
    their old snapshot (see the module docstring for the freshness
    contract).

    Args:
        name: Dataset name.
        uri: Dataset storage root override.
        **data: Fields to update in the model.

    Returns:
        The updated model.
    """
    store = _dataset_store(name, uri)
    # FIXME this triggers the ensure zfs
    DatasetHandle(name, dataset_uri(name, uri))
    model = _load_model(store, name, **data)
    factories.get_versions(name, uri).make(path.CONFIG, model)
    factories.clear_caches()
    log.info("Updated dataset config", dataset=name, uri=mask_uri(store.uri))
    return model


def ensure_dataset(name: str, uri: Uri | None = None, **data: Any) -> DatasetModel:
    """Get or create a dataset.

    Creates ``config.yml`` if the dataset doesn't exist, recording ``data``
    at creation (e.g. ``ensure_dataset("big_leak", shards=8)``); ``data`` is
    ignored when the dataset already exists.

    Args:
        name: Dataset name.
        uri: Dataset storage root override.
        **data: Config data recorded at creation.

    Returns:
        The dataset's model.
    """
    if dataset_exists(name, uri):
        return get_dataset_model(name, uri)
    model = update_dataset(name, uri, **data)
    log.info("Created dataset", dataset=name)
    return model


class Catalog:
    """Multi-dataset lakehouse catalog – enumeration and dataset addressing.

    Example:
        ```python
        from ftm_lakehouse import get_entities, get_lakehouse

        catalog = get_lakehouse()
        for name in catalog.list_datasets():
            print(name, get_entities(name).get_statistics())
        ```
    """

    def __init__(self, uri: Uri) -> None:
        self.uri = uri
        self._log = get_logger(__name__, catalog=mask_uri(uri))

    def __repr__(self) -> str:
        return f"Catalog({mask_uri(self.uri)!r})"

    @cached_property
    def _store(self) -> Store:
        """Raw storage access."""
        return get_store(uri=ensure_api_uri(self.uri), serialization_mode="raw")

    def dataset_uri(self, name: str) -> str:
        """Validated canonical uri for ``name`` under this catalog's root.

        Raises:
            ValueError: If ``name`` is not a valid dataset name.
        """
        return dataset_uri(name, join_uri(self.uri, name))

    def list_datasets(self) -> Generator[str, None, None]:
        """Yield the names of all datasets that have a ``config.yml``."""
        for child in self._store._fs.ls(self.uri):
            name = Path(child).name
            if self._store.exists(f"{name}/{path.CONFIG}"):
                yield name

    def ensure_dataset(self, name: str, **data: Any) -> DatasetModel:
        """Get or create a dataset under this catalog (see :func:`ensure_dataset`)."""
        return ensure_dataset(name, self.dataset_uri(name), **data)
