"""Tests for semantic API DoS limits: entity_ids cap, filter-key cap.

Guards the trust boundary at the FastAPI layer: no single tenant request should
be able to build an unbounded SQL IN clause or fan out into a huge ftmq filter
expression. Transport-level concerns (body byte size, per-request timeout) are
deliberately left to the reverse proxy (``client_max_body_size``,
``proxy_read_timeout``) – consistent with the project's auth / rate-limiting
philosophy.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from ftmq.query import G, M, P, Query

from ftm_lakehouse.api import dependencies
from ftm_lakehouse.api.main import get_app
from ftm_lakehouse.core.arrow import ARROW_CONTENT_TYPE, serialize_table
from ftm_lakehouse.model.statement import LakehouseStatement, statements_to_arrow


@pytest.fixture()
def client(tmp_path) -> TestClient:
    app = get_app(lake_uri=str(tmp_path))
    return TestClient(app)


def _error_messages(response) -> str:
    """Concatenate Pydantic's per-error ``msg`` strings into one searchable
    blob (the FastAPI 422 shape is ``{"detail": [{"msg": ..., ...}, ...]}``)."""
    return " | ".join(err["msg"] for err in response.json()["detail"])


def test_entities_query_rejects_too_many_entity_ids(client, monkeypatch) -> None:
    monkeypatch.setattr(dependencies.api_settings, "query_max_in_values", 5)
    query = Query().where(M(entity_id__in=[f"e{i}" for i in range(6)]))
    body = {"query": query.to_dict()}
    response = client.post("/test_ds/_api/entities/query", json=body)
    assert response.status_code == 422
    assert "entity_id__in" in _error_messages(response)


def test_entities_query_rejects_unknown_body_keys(client) -> None:
    """The legacy flat filter-kwargs format fails loudly (extra="forbid")
    instead of silently streaming an unfiltered result."""
    body = {"schema": "Person", "prop": "name", "value": "v"}
    response = client.post("/test_ds/_api/entities/query", json=body)
    assert response.status_code == 422

    response = client.post(
        "/test_ds/_api/entities/statements/query", json={"schema": "Person"}
    )
    assert response.status_code == 422


def test_query_rejects_too_many_conditions(client, monkeypatch) -> None:
    """The leaf-count cap applies to the parsed tree, not body keys."""
    monkeypatch.setattr(dependencies.api_settings, "query_max_filter_keys", 2)
    q = Query(P(name="x"), M(schema="Person"), G(countries="de"))
    response = client.post("/test_ds/_api/entities/query", json={"query": q.to_dict()})
    assert response.status_code == 422
    assert "filter conditions" in response.json()["detail"][0]["msg"]


def test_query_rejects_oversized_id_list(client, monkeypatch) -> None:
    """An `in(entity_id, (...))` list cannot bypass the entity_ids cap"""
    monkeypatch.setattr(dependencies.api_settings, "query_max_in_values", 5)
    q = Query(M(entity_id__in=[str(i) for i in range(6)]))
    response = client.post("/test_ds/_api/entities/query", json={"query": q.to_dict()})
    assert response.status_code == 422
    assert "maximum" in response.json()["detail"][0]["msg"]


def test_query_rejects_malformed(client) -> None:
    """Malformed maps to a 400, before any streaming starts."""
    for endpoint in ("query", "statements/query"):
        response = client.post(
            f"/test_ds/_api/entities/{endpoint}", json={"query": {"foo": "bar"}}
        )
        assert response.status_code == 422, endpoint
        assert "Invalid query json" in response.json()["detail"][0]["msg"]


def _statement_batch() -> pa.Table:
    stmt = LakehouseStatement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset="test",
    )
    return statements_to_arrow([stmt], datetime.now(timezone.utc))


def _post_bulk(client: TestClient, table: pa.Table):
    return client.post(
        "/test/_api/journal/bulk",
        content=serialize_table(table),
        headers={"Content-Type": ARROW_CONTENT_TYPE},
    )


def test_journal_bulk_accepts_a_statement_batch(client) -> None:
    assert _post_bulk(client, _statement_batch()).status_code == 200


def test_journal_bulk_rejects_a_foreign_batch(client) -> None:
    """A batch missing statement columns is a client error, not a 500."""
    res = _post_bulk(client, pa.table({"nope": ["x"]}))
    assert res.status_code == 400


def test_journal_bulk_rejects_a_null_required_column(client) -> None:
    """Nulls where the statement schema requires a value are a client error."""
    table = _statement_batch()
    holed = table.set_column(
        table.schema.get_field_index("entity_id"),
        pa.field("entity_id", pa.string()),
        pa.array([None], pa.string()),
    )
    res = _post_bulk(client, holed)
    assert res.status_code == 400


def test_journal_bulk_rejects_a_non_arrow_body(client) -> None:
    res = client.post(
        "/test/_api/journal/bulk",
        content=b"not arrow at all",
        headers={"Content-Type": ARROW_CONTENT_TYPE},
    )
    assert res.status_code == 400
