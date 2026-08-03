from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri

from ftm_lakehouse.core.api import LakehouseApiMixin, ensure_api_uri
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


class BaseRepository(LakehouseApiMixin):
    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(uri)
        self.dataset = dataset
        self.uri = uri
        self._store_uri = ensure_api_uri(uri)
        self._store = get_store(self._store_uri, serialization_mode="raw")
        self._model = DatasetModel(**load_config(self._store, name=self.dataset))
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            storage=self.uri,
        )
        self._tags = TagStore(self._store_uri)
        self._versions = VersionStore(self._store_uri)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
