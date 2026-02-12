from typing import Iterable

import orjson
from followthemoney import Statement, StatementEntity
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.types import StatementEntities, Statements
from ftmq.util import ensure_entity

from ftm_lakehouse.core.api import LakehouseApiMixin, require_api


class ApiEntityRepository(LakehouseApiMixin):
    """Patch methods for EntityRepository if running against http api"""

    dataset: str

    @require_api
    def _make_url(self, endpoint: str) -> str:
        return self._api.make_url(f"_api/entities/{endpoint}")

    @require_api
    def _api_flush(self) -> int:
        url = self._make_url("flush")
        res = self._api.make_request(url, "POST")
        return int(res.text)

    @require_api
    def _api_query(
        self,
        entity_ids: Iterable[str] | None = None,
        flush_first: bool = False,
        **filters,
    ) -> StatementEntities:
        url = self._make_url("query")
        data = {
            "entity_ids": list(entity_ids) if entity_ids else [],
            "flush_first": flush_first,
            **filters,
        }
        for line in self._api.stream_request(url, "POST", json=data):
            yield ensure_entity(orjson.loads(line), StatementEntity)

    @require_api
    def _api_query_statements(self, q: Query | None = None) -> Statements:
        q = q or Query()
        data = q.to_dict()
        url = self._make_url("statements/query")
        for line in self._api.stream_request(url, "POST", json=data):
            yield Statement.from_dict(orjson.loads(line))

    @require_api
    def _api_delete_entity(self, entity_id: str) -> int:
        url = self._make_url(entity_id)
        res = self._api.make_request(url, "DELETE")
        return int(res.text)

    @require_api
    def _api_stats(self) -> DatasetStats:
        url = self._make_url("stats")
        res = self._api.make_request(url)
        return DatasetStats(**res.json())

    @require_api
    def _api_version(self) -> int | None:
        url = self._make_url("statements/version")
        res = self._api.make_request(url)
        text = res.text.strip()
        return int(text) if text else None
