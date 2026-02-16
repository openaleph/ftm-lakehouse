"""ApiJournalStore - HTTP API journal backed by JSONL wire format."""

from datetime import datetime

import orjson
from anystore.logging import get_logger

from ftm_lakehouse.core.api import LakehouseApiMixin
from ftm_lakehouse.storage.journal.base import (
    BaseJournalStore,
    BaseJournalWriter,
    JournalRow,
    JournalRows,
)

log = get_logger(__name__)

JSONL_CONTENT_TYPE = "application/x-ndjson"


def _from_iso(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _to_iso(value: str | datetime | None) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return value.isoformat()


def serialize_row(row: dict[str, str | datetime | None]) -> bytes:
    parts = [
        row["id"],
        row["bucket"],
        row["origin"],
        row["canonical_id"],
        row["data"],
        _to_iso(row["deleted_at"]),
    ]
    return orjson.dumps(parts)


def serialize_rows(rows: list[dict]) -> bytes:
    """Serialize journal row dicts as JSONL."""
    return b"\n".join(map(serialize_row, rows))


def deserialize_row(line: str) -> JournalRow:
    """Deserialize a JSONL line into a JournalRow."""
    id, bucket, origin, canonical_id, data, deleted_at = orjson.loads(line)
    return id, bucket, origin, canonical_id, data, _from_iso(deleted_at)


class ApiJournalWriter(BaseJournalWriter["ApiJournalStore"]):
    def _upsert_batch(self) -> None:
        if not self.batch:
            return
        payload = serialize_rows(list(self.batch.values()))
        self.batch = {}
        url = self.store._make_url("bulk")
        self.store._api.make_request(
            url,
            "POST",
            content=payload,
            headers={"Content-Type": JSONL_CONTENT_TYPE},
        )


class ApiJournalStore(BaseJournalStore[ApiJournalWriter], LakehouseApiMixin):
    _writer_cls = ApiJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        BaseJournalStore.__init__(self, dataset, uri)
        LakehouseApiMixin.__init__(self, self.uri)

    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"{self.dataset}/_api/journal/{endpoint}")

    def iterate(self, *args, **kwargs) -> JournalRows:
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
