from anystore.logging import get_logger
from anystore.types import Uri

from ftm_lakehouse.core.api import LakehouseApiMixin
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


def _to_anystore_uri(uri: Uri) -> Uri:
    """Convert http[s]:// URIs to anystore+http[s]:// for ApiFileSystem support."""
    uri_str = str(uri)
    if uri_str.startswith("https://"):
        return f"anystore+{uri_str}"
    if uri_str.startswith("http://"):
        return f"anystore+{uri_str}"
    return uri


class BaseRepository(LakehouseApiMixin):
    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(uri)
        self.dataset = dataset
        self.uri = uri
        self._store_uri = _to_anystore_uri(uri)
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            storage=self.uri,
        )
        self._tags = TagStore(self._store_uri)
        self._versions = VersionStore(self._store_uri)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
