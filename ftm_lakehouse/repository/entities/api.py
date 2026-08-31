"""Api-mode ``EntityRepository`` – the http twin picked at construction."""

from typing import Iterator

import orjson
from anystore.types import Uri
from followthemoney import StatementEntity
from followthemoney.statement import StatementDict
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.types import StatementEntities, Statements
from ftmq.util import ensure_entity

from ftm_lakehouse.model.statement import LakehouseStatement
from ftm_lakehouse.repository.entities.main import EntityRepository
from ftm_lakehouse.util import validate_origin


class ApiEntityRepository(EntityRepository):
    """``EntityRepository`` against a remote lakehouse api.

    Resolved by [`get_entities`][ftm_lakehouse.repository.factories.get_entities] for
    http uris – the same construction-time pick ``get_journal`` does for the
    journal. Overrides the api-capable methods with their http delegations
    under the public names; everything ``@no_api`` stays guarded by the
    inherited decorator.
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        if not self._is_api:
            raise RuntimeError(
                f"`{type(self).__name__}` requires an http uri – resolve the "
                "repository via `get_entities()`"
            )

    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"_api/entities/{endpoint}")

    def flush(self) -> int:
        url = self._make_url("flush")
        res = self._api.make_request(url, "POST")
        return int(res.text)

    def merge(self, force: bool = False) -> None:
        url = self._make_url("merge")
        self._api.make_request(url, "POST", params={"force": force})

    def query(
        self,
        q: Query | None = None,
        *,
        flush_first: bool = False,
    ) -> StatementEntities:
        url = self._make_url("query")
        data = {"flush_first": flush_first, "query": q.to_dict() if q else None}
        for line in self._api.stream_request(url, "POST", json=data):
            yield ensure_entity(orjson.loads(line), StatementEntity)

    def query_statements(
        self,
        q: Query | None = None,
        *,
        flush_first: bool = False,
    ) -> Statements:
        for data in self._stream_statements(q, flush_first):
            yield LakehouseStatement.from_dict(data)

    def query_statements_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """The wire rows as they arrive – no ``LakehouseStatement`` round-trip."""
        return self._stream_statements(q, False)

    def _stream_statements(
        self, q: Query | None, flush_first: bool
    ) -> Iterator[StatementDict]:
        url = self._make_url("statements/query")
        data = {"flush_first": flush_first, "query": q.to_dict() if q else None}
        for line in self._api.stream_request(url, "POST", json=data):
            yield orjson.loads(line)

    def delete_entity(self, entity_id: str, origin: str | None = None) -> int:
        url = self._make_url(entity_id)
        params = {"origin": validate_origin(origin)} if origin else None
        res = self._api.make_request(url, "DELETE", params=params)
        return int(res.text)

    def delete_origin(self, origin: str) -> None:
        # validate here too so a bad origin fails locally with the same
        # `ValueError` the local repository raises, not as a remote 400
        url = self._make_url(f"origins/{validate_origin(origin)}")
        self._api.make_request(url, "DELETE")

    def stats(self) -> DatasetStats:
        url = self._make_url("stats")
        res = self._api.make_request(url)
        return DatasetStats(**res.json())

    @property
    def version(self) -> int | None:
        url = self._make_url("statements/version")
        res = self._api.make_request(url)
        text = res.text.strip()
        return int(text) if text else None
