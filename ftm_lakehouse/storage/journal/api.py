"""ApiJournalStore - HTTP API journal speaking the Arrow IPC stream format."""

import pyarrow as pa

from ftm_lakehouse.core.api import LakehouseApiMixin
from ftm_lakehouse.core.arrow import ARROW_CONTENT_TYPE, serialize_table
from ftm_lakehouse.storage.journal.base import BaseJournalStore, BaseJournalWriter


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
    """Client side of a remote journal – it writes; the server flushes.

    The mixin comes first so its ``_is_api`` wins over the base's default.
    """

    _writer_cls = ApiJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        BaseJournalStore.__init__(self, dataset, uri)
        LakehouseApiMixin.__init__(self, self.uri)

    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"{self.dataset}/_api/journal/{endpoint}")

    def close(self) -> None:
        self._api.client.close()
