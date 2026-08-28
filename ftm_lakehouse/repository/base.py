from functools import cache
from typing import NamedTuple

from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import ensure_uri, join_uri, mask_uri

from ftm_lakehouse.core.api import LakehouseApiMixin, ensure_api_uri, get_api
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs import ensure_zfs_dataset
from ftm_lakehouse.model.dataset import get_model_class
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore
from ftm_lakehouse.util import validate_dataset_name


class DatasetRef(NamedTuple):
    """A dataset address – the ``(name, uri)`` pair everything resolves from.

    Unpacks like a plain tuple (``name, uri = ref``); repositories are
    resolved from it via the factories. The rich per-dataset object is
    `DatasetHandle`.
    """

    name: str
    uri: str


def dataset_uri(dataset: str, uri: Uri | None = None) -> str:
    """Canonical URI for a dataset – same location, same string, same cache key.

    Validates ``dataset`` first
    (`validate_dataset_name`) – every repository
    factory and operation resolves through here, so no caller-supplied name
    reaches path construction unchecked. ``None`` derives
    ``{LAKEHOUSE_URI}/{dataset}`` exactly like
    [`get_lakehouse`][ftm_lakehouse.lake.get_lakehouse] does for the catalog; explicit
    values (str or ``Path``) are normalized via ``ensure_uri``.

    Raises:
        ValueError: If ``dataset`` is not a valid dataset name.
    """
    validate_dataset_name(dataset)
    if uri is not None:
        return str(ensure_uri(uri))
    settings = Settings()
    return str(join_uri(ensure_uri(settings.uri), dataset))


@cache
def ensure_zfs(dataset: str, uri: Uri) -> None:
    """Provision the dataset's tuned ZFS datasets for its storage location.

    Resolves the store and api client from ``uri`` (both cached upstream, so
    this is free for callers that hold them anyway). No-op unless the store
    is local and ``LAKEHOUSE_ON_ZFS`` is set; for http uris the remote
    ``_api/ensure`` endpoint is triggered instead. ``ensure_zfs_dataset``
    itself is cached per ``(pool, dataset)``, so this fires actual ``zfs``
    commands once per process. Runs at repository construction and on
    catalog config writes.

    Raises:
        RuntimeError: When ZFS mode is on but no pool is configured.
    """
    store = get_store(ensure_api_uri(uri), serialization_mode="raw")
    api = get_api(uri)
    settings = Settings()
    if store.is_local and settings.on_zfs:
        if settings.zfs_pool is None:
            raise RuntimeError("Configure LAKEHOUSE_ZFS_POOL for zfs integration!")
        ensure_zfs_dataset(settings.zfs_pool, dataset)
    elif api is not None:  # trigger api
        api.ensure()


class DatasetHandle(LakehouseApiMixin):
    """Dataset-addressed handle base: identity, config snapshot, store, tags,
    versions and the api client.

    Combines no storage itself – the repositories layer their storage
    combinations on top, and
    [`DatasetJobOperation`][ftm_lakehouse.operation.base.DatasetJobOperation]
    adds the job lifecycle. Anything that addresses one dataset subclasses
    this.
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(uri)
        self.dataset = validate_dataset_name(dataset)
        self.uri = uri
        self._store_uri = ensure_api_uri(uri)
        self._store = get_store(self._store_uri, serialization_mode="raw")
        ensure_zfs(self.dataset, uri)
        self._model = get_model_class()(**load_config(self._store, name=self.dataset))
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            storage=mask_uri(self.uri),
        )
        self._tags = TagStore(self._store_uri)
        self._versions = VersionStore(self._store_uri)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
