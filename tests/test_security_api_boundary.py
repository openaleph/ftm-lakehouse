"""Tests for semantic API DoS limits: entity_ids cap, filter-key cap.

Guards the trust boundary at the FastAPI layer: no single tenant request should
be able to build an unbounded SQL IN clause or fan out into a huge ftmq filter
expression. Transport-level concerns (body byte size, per-request timeout) are
deliberately left to the reverse proxy (``client_max_body_size``,
``proxy_read_timeout``) – consistent with the project's auth / rate-limiting
philosophy.
"""

import pytest
from fastapi.testclient import TestClient

from ftm_lakehouse.api import dependencies
from ftm_lakehouse.api.main import get_app


@pytest.fixture()
def client(tmp_path) -> TestClient:
    app = get_app(lake_uri=str(tmp_path))
    return TestClient(app)


def _error_messages(response) -> str:
    """Concatenate Pydantic's per-error ``msg`` strings into one searchable
    blob (the FastAPI 422 shape is ``{"detail": [{"msg": ..., ...}, ...]}``)."""
    return " | ".join(err["msg"] for err in response.json()["detail"])


def test_entities_query_rejects_too_many_entity_ids(client, monkeypatch) -> None:
    monkeypatch.setattr(dependencies.api_settings, "max_entity_ids", 5)
    body = {"entity_ids": [f"e{i}" for i in range(6)]}
    response = client.post("/test_ds/_api/entities/query", json=body)
    assert response.status_code == 422
    assert "entity_ids" in _error_messages(response)


def test_entities_query_rejects_non_list_entity_ids(client) -> None:
    response = client.post(
        "/test_ds/_api/entities/query", json={"entity_ids": "not-a-list"}
    )
    assert response.status_code == 422
    # Pydantic's auto-generated type error – contract is "non-list rejected".
    assert "list" in _error_messages(response).lower()


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


def test_query_rejects_too_many_rql_conditions(client, monkeypatch) -> None:
    """The leaf-count cap applies to the parsed RQL tree, not body keys."""
    monkeypatch.setattr(dependencies.api_settings, "max_filter_keys", 2)
    rql = "and(eq(schema,Person),eq(properties.name,x),eq(countries,de))"
    response = client.post("/test_ds/_api/entities/query", json={"query": rql})
    assert response.status_code == 400
    assert "filter conditions" in response.json()["detail"]


def test_query_rejects_oversized_rql_id_list(client, monkeypatch) -> None:
    """An `in(entity_id, (...))` list cannot bypass the entity_ids cap by
    riding in the RQL string."""
    monkeypatch.setattr(dependencies.api_settings, "max_entity_ids", 5)
    ids = ",".join(f"e{i}" for i in range(6))
    response = client.post(
        "/test_ds/_api/entities/query", json={"query": f"in(entity_id,({ids}))"}
    )
    assert response.status_code == 400
    assert "maximum" in response.json()["detail"]


def test_query_rejects_malformed_rql(client) -> None:
    """Malformed RQL maps to a 400, before any streaming starts."""
    for endpoint in ("query", "statements/query"):
        response = client.post(
            f"/test_ds/_api/entities/{endpoint}", json={"query": "in(entity_id,"}
        )
        assert response.status_code == 400, endpoint
        assert "Invalid RQL" in response.json()["detail"]
