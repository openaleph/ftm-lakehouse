from unittest.mock import patch

from fastapi.testclient import TestClient

from ftm_lakehouse.api.auth import create_access_token, settings
from ftm_lakehouse.api.main import get_app
from ftm_lakehouse.core.settings import __version__
from ftm_lakehouse.operation.crawl import crawl

DATASET = "tmp_dataset"


def _auth_header(methods=None, prefixes=None):
    token = create_access_token(
        methods=methods or ["*"],
        prefixes=prefixes or ["/"],
    )
    return {"Authorization": f"Bearer {token}"}


def test_api(fixtures_path, tmp_catalog):
    dataset = tmp_catalog.get_dataset(DATASET)
    dataset.ensure()
    crawl(
        dataset,
        fixtures_path / "src",
        make_entities=True,
    )

    app = get_app(lake_uri=tmp_catalog.uri)
    client = TestClient(app)

    # unauthenticated requests are rejected
    res = client.get(f"{DATASET}/archive/")
    assert res.status_code == 401

    # authenticated: list keys
    auth = _auth_header()
    res = client.get(f"{DATASET}/archive/", headers=auth)
    assert res.status_code == 200
    keys = res.text.strip().split("\n")
    assert len(keys) > 0

    # head for existing file
    key = f"{DATASET}/archive/{keys[0]}"
    res = client.head(key, headers=auth)
    assert res.status_code == 200
    assert "Content-Length" in res.headers

    # get streams content
    res = client.get(key, headers=auth)
    assert res.status_code == 200
    assert len(res.content) > 0

    # non-existent key
    res = client.head(f"{DATASET}/archive/nonexistent", headers=auth)
    assert res.status_code == 404

    # restricted token: read-only
    read_auth = _auth_header(methods=["GET", "HEAD"])
    res = client.get(key, headers=read_auth)
    assert res.status_code == 200
    res = client.put(key, headers=read_auth, content=b"test")
    assert res.status_code == 403  # PUT not allowed

    # restricted token: prefix-scoped
    scoped_auth = _auth_header(prefixes=[f"/{DATASET}/archive/"])
    res = client.head(key, headers=scoped_auth)
    assert res.status_code == 200
    res = client.head(f"{DATASET}/tags/foo", headers=scoped_auth)
    assert res.status_code == 403  # outside prefix

    # expired token
    expired = create_access_token(methods=["*"], prefixes=["/"], exp=-1)
    res = client.head(key, headers={"Authorization": f"Bearer {expired}"})
    assert res.status_code == 401


def test_api_public_mode(fixtures_path, tmp_catalog):
    dataset = tmp_catalog.get_dataset(DATASET)
    dataset.ensure()
    crawl(
        dataset,
        fixtures_path / "src",
        make_entities=True,
    )

    app = get_app(lake_uri=tmp_catalog.uri)
    client = TestClient(app)

    # discover a key with auth first
    auth = _auth_header()
    res = client.get(f"{DATASET}/archive/", headers=auth)
    assert res.status_code == 200
    keys = res.text.strip().split("\n")
    assert len(keys) > 0
    key = f"{DATASET}/archive/{keys[0]}"

    with patch.object(settings, "auth_required", False):
        # write methods are rejected in public mode
        res = client.put(key, content=b"test")
        assert res.status_code == 403

        # GET without token works
        res = client.get(key)
        assert res.status_code == 200
        assert len(res.content) > 0

        # HEAD without token works
        res = client.head(key)
        assert res.status_code == 200
        assert "Content-Length" in res.headers


def test_api_version_header(tmp_catalog):
    app = get_app(lake_uri=tmp_catalog.uri)
    client = TestClient(app)
    auth = _auth_header()
    res = client.get("/", headers=auth)
    assert res.headers["X-Lakehouse-Version"] == __version__
