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


def test_entities_query_rejects_too_many_filter_keys(client, monkeypatch) -> None:
    monkeypatch.setattr(dependencies.api_settings, "max_filter_keys", 3)
    body = {
        "entity_ids": ["e1"],
        "flush_first": False,
        "schema": "Person",
        "origin": "x",
        "prop": "name",
        "value": "v",
    }
    response = client.post("/test_ds/_api/entities/query", json=body)
    assert response.status_code == 422
    assert "filter keys" in _error_messages(response)


def test_statements_query_rejects_too_many_filter_keys(client, monkeypatch) -> None:
    monkeypatch.setattr(dependencies.api_settings, "max_filter_keys", 2)
    body = {"schema": "Person", "origin": "x", "prop": "name"}
    response = client.post("/test_ds/_api/entities/statements/query", json=body)
    assert response.status_code == 422
