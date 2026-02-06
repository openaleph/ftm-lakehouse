"""ApiJournalStore - HTTP API journal backed by TSV wire format."""

from datetime import datetime
import httpx
from anystore.logging import get_logger
from anystore.util import join_relpaths, join_uri

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
    return f'{row["id"]}\t{row["bucket"]}\t{row["origin"]}\t{row["canonical_id"]}\t{row["data"]}\t{_to_iso(row["deleted_at"])}'.encode()


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
        url = self.store.make_url("bulk")
        self.store.make_request(
            url,
            "POST",
            content=serialize_rows(self.batch),
            headers={"Content-Type": TSV_CONTENT_TYPE},
        )
        self.batch = []


class ApiJournalStore(BaseJournalStore[ApiJournalWriter]):
    _writer_cls = ApiJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        super().__init__(dataset, uri)
        self.client = httpx.Client()

    def make_url(self, endpoint: str) -> str:
        url = join_relpaths(self.dataset, "journal", endpoint)
        return join_uri(self.uri, url)

    def make_request(
        self, url: str, method: str = "GET", **kwargs  # noqa: ANN003
    ) -> httpx.Response:
        res = self.client.request(method, url, **kwargs)
        res.raise_for_status()
        return res

    def iterate(self) -> JournalRows:
        url = self.make_url("iterate")
        yield from self._iterate_stream(url)

    def flush(self) -> JournalRows:
        url = self.make_url("flush")
        yield from self._iterate_stream(url, "POST")

    def count(self) -> int:
        url = self.make_url("count")
        res = self.make_request(url)
        return int(res.text)

    def clear(self) -> int:
        url = self.make_url("clear")
        res = self.make_request(url, "DELETE")
        return int(res.text)

    def close(self) -> None:
        self.client.close()

    def _iterate_stream(self, url: str, method: str = "GET") -> JournalRows:
        with self.client.stream(method, url) as stream:
            for line in stream.iter_lines():
                yield deserialize_row(line)
