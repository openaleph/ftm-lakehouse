"""Tests for dataset-name validation across API, CLI, and library entry points.

Guards the trust boundary: every external entry point must reject malformed
dataset names before they reach path construction, SQL identifiers, or DuckDB
queries downstream.
"""

import pytest
from fastapi.testclient import TestClient

from ftm_lakehouse.api.dependencies import get_dataset as api_get_dataset
from ftm_lakehouse.api.main import get_app
from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.util import RESERVED_DATASET_NAMES, validate_dataset_name


@pytest.mark.parametrize(
    "name",
    [
        "my_dataset",
        "a",
        "abc_123",
        "x_y_z_0",
    ],
)
def test_validate_dataset_name_accepts_valid(name: str) -> None:
    assert validate_dataset_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        "../etc/passwd",
        "foo/bar",
        "foo bar",
        "foo'bar",
        'foo"bar',
        "FOOBAR",
        "foo;bar",
        "foo\x00bar",
        "../../etc",
        "foo\nbar",
    ],
)
def test_validate_dataset_name_rejects_invalid(name: str) -> None:
    with pytest.raises(ValueError):
        validate_dataset_name(name)


@pytest.mark.parametrize("name", sorted(RESERVED_DATASET_NAMES))
def test_validate_dataset_name_rejects_reserved(name: str) -> None:
    with pytest.raises(ValueError, match="reserved"):
        validate_dataset_name(name)


def test_catalog_get_dataset_rejects_invalid(tmp_path) -> None:
    catalog = Catalog(uri=str(tmp_path))
    with pytest.raises(ValueError):
        catalog.get_dataset("../escape")
    with pytest.raises(ValueError):
        catalog.get_dataset("catalog")


def test_api_get_dataset_dependency_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        # request is unused for invalid names — validator raises first
        api_get_dataset("../etc", request=None)  # type: ignore[arg-type]


def test_api_returns_400_on_invalid_dataset_name(tmp_path) -> None:
    """End-to-end: an invalid dataset name in the URL produces a 400."""
    app = get_app(lake_uri=str(tmp_path))
    client = TestClient(app)

    response = client.get("/catalog/_api/entities/stats")
    assert response.status_code == 400
    assert "reserved" in response.json()["detail"].lower()

    response = client.get("/foo%20bar/_api/entities/stats")
    assert response.status_code == 400
