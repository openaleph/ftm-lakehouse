"""ApiJournalStore - HTTP API journal speaking the Arrow IPC stream format."""

import pyarrow as pa
from anystore.logging import get_logger

from ftm_lakehouse.core.api import LakehouseApiMixin
from ftm_lakehouse.core.arrow import ARROW_CONTENT_TYPE, serialize_table
from ftm_lakehouse.storage.journal.base import BaseJournalStore, BaseJournalWriter

log = get_logger(__name__)


class ApiJournalWriter(BaseJournalWriter["ApiJournalStore"]):
    def _insert(self, batch: pa.Table) -> None:
        url = self.store._make_url("bulk")
        self.store._api.make_request(
            url,
            "POST",
            content=serialize_table(batch),
            headers={"Content-Type": ARROW_CONTENT_TYPE},
        )


class ApiJournalStore(LakehouseApiMixin, BaseJournalStore[ApiJournalWriter]):
    """The client side of a remote journal – writes, counts, clears.

    Flushing is not part of it: the store that holds the rows drains them
    (`BaseJournalStore.flush_batches` is ``@no_api``), and a repository
    in api mode delegates its whole flush to the server. The mixin comes
    first so its ``_is_api`` wins over the base's default.
    """

    _writer_cls = ApiJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        BaseJournalStore.__init__(self, dataset, uri)
        LakehouseApiMixin.__init__(self, self.uri)

    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"{self.dataset}/_api/journal/{endpoint}")

    def count(self) -> int:
        url = self._make_url("count")
        res = self._api.make_request(url)
        return int(res.text)

    def clear(self) -> int:
        url = self._make_url("clear")
        res = self._api.make_request(url, "DELETE")
        return int(res.text)

    def close(self) -> None:
        self._api.client.close()
