from typing import Any

import orjson
from followthemoney import StatementEntity
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import LakeStatement
from ftmq.types import StatementEntities, Statements
from ftmq.util import ensure_entity

from ftm_lakehouse.core.api import LakehouseApiMixin, require_api
from ftm_lakehouse.core.settings import Settings

settings = Settings()


def _serialize_query(q: Query | None) -> dict[str, Any]:
    """Wire form of a ``Query`` for the lakehouse query endpoints.

    The filter tree travels as RQL (``query``); ``to_rql`` does not carry
    ordering or slicing, so those ride as sibling fields the server
    recombines (:meth:`ftm_lakehouse.api.dependencies.QueryBody.to_query`).
    """
    return {
        "query": q.to_rql() if q else None,
        "order_by": q.sort.serialize() if q and q.sort else None,
        "limit": q.limit if q else None,
        "offset": q.offset if q else None,
    }


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
    def _api_merge(self, grace_period_days: int | None = None) -> None:
        url = self._make_url("merge")
        if grace_period_days is None:
            grace_period_days = settings.grace_period_days
        self._api.make_request(
            url, "POST", json={"grace_period_days": grace_period_days}
        )

    @require_api
    def _api_query(
        self,
        q: Query | None = None,
        *,
        flush_first: bool = False,
    ) -> StatementEntities:
        url = self._make_url("query")
        data = {
            "flush_first": flush_first,
            **_serialize_query(q),
        }
        for line in self._api.stream_request(url, "POST", json=data):
            yield ensure_entity(orjson.loads(line), StatementEntity)

    @require_api
    def _api_query_statements(self, q: Query | None = None) -> Statements:
        url = self._make_url("statements/query")
        for line in self._api.stream_request(url, "POST", json=_serialize_query(q)):
            yield LakeStatement.from_dict(orjson.loads(line))

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
