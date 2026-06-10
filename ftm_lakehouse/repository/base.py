from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri

from ftm_lakehouse.core.api import LakehouseApiMixin, ensure_api_uri
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


def resolve_shards(uri: Uri) -> int:
    """Shard count for the dataset at ``uri``.

    Reads the dataset's recorded ``config.yml`` value, falling back to
    :data:`DEFAULT_SHARDS` when no config exists yet (fresh dataset). The
    dataset's own config is the single source of truth – there is
    deliberately no environment override, so a process with a different
    environment cannot mis-shard an existing dataset (``shards`` is
    immutable after the first write).
    """
    store = get_store(ensure_api_uri(uri), serialization_mode="raw")
    return int(load_config(store).get("shards") or DEFAULT_SHARDS)


class BaseRepository(LakehouseApiMixin):
    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(uri)
        self.dataset = dataset
        self.uri = uri
        self._store_uri = ensure_api_uri(uri)
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            storage=self.uri,
        )
        self._tags = TagStore(self._store_uri)
        self._versions = VersionStore(self._store_uri)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
