"""The entities api answers what the local repository answers."""

import orjson
import pytest
from fastapi.testclient import TestClient
from ftmq.util import make_entity

from ftm_lakehouse.api.main import get_app
from ftm_lakehouse.catalog import ensure_dataset
from ftm_lakehouse.repository import EntityRepository
from ftm_lakehouse.repository.factories import get_entities

DATASET = "api_entities"


@pytest.fixture()
def app(tmp_path):
    app = get_app(lake_uri=str(tmp_path))
    ensure_dataset(DATASET, uri=str(app.state.lake.dataset_uri(DATASET)))
    return app


def _repo(app) -> EntityRepository:
    return get_entities(DATASET, app.state.lake.dataset_uri(DATASET))


def _write(repo: EntityRepository) -> None:
    repo.add(
        make_entity({"id": "p1", "schema": "Person", "properties": {"name": ["Jane"]}})
    )
    repo.flush()


def test_api_statements_rows_match_local(app):
    """The statements route streams the rows `query_statements_data` yields –
    the same columns and values, so ``statements iterate`` writes the same csv
    in api mode."""
    repo = _repo(app)
    _write(repo)
    res = TestClient(app).post(
        f"/{DATASET}/_api/entities/statements/query", json={"query": None}
    )
    assert res.status_code == 200
    wire = sorted((orjson.loads(line) for line in res.text.splitlines()), key=str)
    assert wire == sorted(repo.query_statements_data(), key=str)


def test_api_version_none_without_table(app):
    """No table is no version – empty on the wire, ``None`` again client-side."""
    client = TestClient(app)
    url = f"/{DATASET}/_api/entities/statements/version"
    assert client.get(url).text == ""
    repo = _repo(app)
    _write(repo)
    assert int(client.get(url).text) == repo.statements.version


def test_api_entities_query_flushes_before_streaming(app, monkeypatch):
    """A failing ``flush_first`` fails the request instead of cutting a 200
    response short."""

    def fail(self):
        raise RuntimeError("flush failed")

    monkeypatch.setattr(EntityRepository, "flush", fail)
    client = TestClient(app, raise_server_exceptions=False)
    res = client.post(
        f"/{DATASET}/_api/entities/query", json={"query": None, "flush_first": True}
    )
    assert res.status_code == 500
