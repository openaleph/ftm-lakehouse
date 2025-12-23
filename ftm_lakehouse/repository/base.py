from anystore.logging import get_logger
from anystore.types import Uri

from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


class BaseRepository:
    def __init__(self, dataset: str, uri: Uri) -> None:
        self.dataset = dataset
        self.uri = uri
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            storage=self.uri,
        )
        self._tags = TagStore(uri)
        self._versions = VersionStore(uri)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
