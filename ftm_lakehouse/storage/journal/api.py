"""ApiJournalStore - HTTP API journal backed by TSV wire format."""

from datetime import datetime

from anystore.logging import get_logger

from ftm_lakehouse.core.api import LakehouseApiMixin
from ftm_lakehouse.storage.journal.base import (
    BaseJournalStore,
    BaseJournalWriter,
    JournalRow,
    JournalRows,
)

log = get_logger(__name__)

TSV_CONTENT_TYPE = "text/tab-separated-values"


def _from_iso(value: str) -> datetime | None:
    if not value:
        return
    return datetime.fromisoformat(value)


def _to_iso(value: str | datetime | None) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return value.isoformat()


def serialize_row(row: dict[str, str | datetime | None]) -> bytes:
    # "id", "bucket", "origin", "canonical_id", "data", "deleted_at"
    parts = (
        row["id"],
        row["bucket"],
        row["origin"],
        row["canonical_id"],
        row["data"],
        _to_iso(row["deleted_at"]),
    )
    return "\t".join(str(p) for p in parts).encode()


def serialize_rows(rows: list[dict]) -> bytes:
    """Serialize journal row dicts as TSV lines."""
    return b"\n".join(map(serialize_row, rows))


def deserialize_row(line: str) -> JournalRow:
    """Deserialize a TSV line into a JournalRow."""
    id, bucket, origin, canonical_id, data, deleted_at = line.split("\t", maxsplit=5)
    return id, bucket, origin, canonical_id, data, _from_iso(deleted_at)


class ApiJournalWriter(BaseJournalWriter["ApiJournalStore"]):
    def _upsert_batch(self) -> None:
        if not self.batch:
            return
        url = self.store._make_url("bulk")
        self.store._api.make_request(
            url,
            "POST",
            content=serialize_rows(self.batch),
            headers={"Content-Type": TSV_CONTENT_TYPE},
        )
        self.batch = []


class ApiJournalStore(BaseJournalStore[ApiJournalWriter], LakehouseApiMixin):
    _writer_cls = ApiJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        BaseJournalStore.__init__(self, dataset, uri)
        LakehouseApiMixin.__init__(self, self.uri)

    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"_api/journal/{endpoint}")

    def iterate(self) -> JournalRows:
        url = self._make_url("iterate")
        for line in self._api.stream_request(url):
            yield deserialize_row(line)

    def flush(self) -> JournalRows:
        url = self._make_url("flush")
        for line in self._api.stream_request(url, "POST"):
            yield deserialize_row(line)

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
