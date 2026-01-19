from fastapi.testclient import TestClient

from ftm_lakehouse.api import app
from ftm_lakehouse.operation.crawl import crawl

DATASET = "tmp_dataset"
SHA1 = "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
PATH = "testdir/test.txt"
URL = f"{DATASET}/{SHA1}"


def _check_headers(res):
    assert "text/plain" in res.headers["content-type"]  # FIXME
    assert res.headers["x-ftm-lakehouse-dataset"] == DATASET
    assert res.headers["x-ftm-lakehouse-path"] == PATH
    assert res.headers["x-ftm-lakehouse-sha1"] == SHA1
    assert res.headers["x-ftm-lakehouse-name"] == "test.txt"
    assert res.headers["x-ftm-lakehouse-size"] == "11"
    return True


def test_api(fixtures_path, tmp_catalog, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_URI", tmp_catalog.uri)
    client = TestClient(app)

    dataset = tmp_catalog.get_dataset(DATASET)
    dataset.ensure()
    crawl(
        dataset.name,
        fixtures_path / "src",
        archive=dataset.archive,
        entities=dataset.entities,
        jobs=dataset.jobs,
        make_entities=True,
    )

    from ftm_lakehouse.api.util import settings

    monkeypatch.setattr(settings, "debug", False)
    # production mode always raises 404 on any errors

    res = client.get("/")
    assert res.status_code == 200

    res = client.head(URL)
    assert _check_headers(res)

    res = client.get(URL)
    assert _check_headers(res)

    # token access
    res = client.get("/file")
    assert res.status_code == 404

    res = client.get(URL + "/token?exp=1")
    token = res.json()["access_token"]
    header = {"Authorization": f"Bearer {token}"}
    res = client.head("/file", headers=header)
    assert res.status_code == 200
    assert _check_headers(res)

    # expired token
    res = client.get(URL + "/token?exp=-1")
    token = res.json()["access_token"]
    header = {"Authorization": f"Bearer {token}"}
    res = client.head("/file", headers=header)
    assert res.status_code == 404

    # invalid requests raise 404
    res = client.head("/foo/bar")
    assert res.status_code == 404
