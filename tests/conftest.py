import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Generator

import boto3
import pytest
import requests
from boto3.resources.base import ServiceResource
from moto.server import ThreadedMotoServer

from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.repository import factories

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="session")
def fixtures_path() -> Path:
    return FIXTURES_PATH


@pytest.fixture(scope="function")
def tmp_catalog(tmp_path) -> Catalog:
    return get_lakehouse(tmp_path)


@pytest.fixture(scope="function")
def tmp_dataset(tmp_path) -> Dataset:
    catalog = get_lakehouse(tmp_path)
    return catalog.get_dataset("tmp_dataset")


@pytest.fixture(autouse=True, scope="function")
def clear_factory_caches():
    """Clear cached factories between tests to prevent cross-test pollution."""
    # Clear before test
    factories.get_archive.cache_clear()
    factories.get_entities.cache_clear()
    factories.get_mappings.cache_clear()
    factories.get_jobs.cache_clear()
    factories.get_versions.cache_clear()
    factories.get_tags.cache_clear()
    yield
    # Clear after test
    factories.get_archive.cache_clear()
    factories.get_entities.cache_clear()
    factories.get_mappings.cache_clear()
    factories.get_jobs.cache_clear()
    factories.get_versions.cache_clear()
    factories.get_tags.cache_clear()


@pytest.fixture(autouse=True, scope="session")
def cleanup_fixtures_tags():
    """Clean up any tags created in fixtures directory during tests."""
    yield
    # Clean up after all tests
    for _dir in (
        FIXTURES_PATH / "lake" / "tmp_dataset" / "tags",
        FIXTURES_PATH / "lake" / "tmp_dataset" / "exports",
        FIXTURES_PATH / "lake" / "tmp_dataset" / "jobs",
    ):
        if _dir.exists():
            shutil.rmtree(_dir)


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "RangeHTTPServer"], cwd=str(FIXTURES_PATH)
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


@pytest.fixture(scope="session", autouse=True)
def http_server():
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()
    return


# http://docs.getmoto.org/en/latest/docs/server_mode.html
@pytest.fixture(scope="session")
def moto_server() -> Generator[ServiceResource, None, None]:
    """Fixture to run a mocked AWS server for testing with some data buckets."""
    server = ThreadedMotoServer(port=8888)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    yield boto3.resource("s3", region_name="us-east-1", endpoint_url=endpoint)
    server.stop()
