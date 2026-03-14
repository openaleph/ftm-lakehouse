import shutil
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import boto3
import pytest
import requests
import uvicorn
from anystore.exceptions import DoesNotExist
from anystore.store import get_store
from boto3.resources.base import ServiceResource
from fastapi import APIRouter, FastAPI
from moto.server import ThreadedMotoServer

from ftm_lakehouse.api.main import _not_found_handler
from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.core.api import get_api
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.repository import factories
from ftm_lakehouse.storage.journal import get_journal

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
    get_journal.cache_clear()
    get_lakehouse.cache_clear()
    yield
    # Clear after test
    factories.get_archive.cache_clear()
    factories.get_entities.cache_clear()
    factories.get_mappings.cache_clear()
    factories.get_jobs.cache_clear()
    factories.get_versions.cache_clear()
    factories.get_tags.cache_clear()
    get_journal.cache_clear()
    get_lakehouse.cache_clear()


@pytest.fixture(autouse=True, scope="session")
def cleanup_fixtures_data():
    """Clean up any data created in fixtures directory during tests."""
    yield
    # Clean up after all tests
    for _dir in (
        FIXTURES_PATH / "lake" / "tmp_dataset" / "tags",
        FIXTURES_PATH / "lake" / "tmp_dataset" / "exports",
        FIXTURES_PATH / "lake" / "tmp_dataset" / "jobs",
        FIXTURES_PATH / "lake" / "tmp_dataset" / "diffs",
        FIXTURES_PATH / "lake" / "new_dataset",
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


@contextmanager
def live_test_api_server(app):
    """Run FastAPI app on a real port for full HTTP integration testing."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@contextmanager
def make_test_api(
    tmp_path: Path,
    routers: list[APIRouter],
) -> Generator[str, None, None]:
    """Create a test FastAPI app with the given routers and yield its base URL.

    Sets up app state (store, lake), mounts routers, registers exception
    handlers, and runs a live uvicorn server.

    Args:
        tmp_path: Root storage directory for the test lake.
        routers: FastAPI routers to mount (archive_router should be last).
    """
    app = FastAPI()
    app.state.store = get_store(tmp_path)
    app.state.lake = get_lakehouse(tmp_path)
    for router in routers:
        app.include_router(router)
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)

    with live_test_api_server(app) as base_url:
        yield base_url

    get_api.cache_clear()
