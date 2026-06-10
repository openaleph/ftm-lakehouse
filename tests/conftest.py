import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
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
from ftm_lakehouse.repository.entities.main import EntityRepository
from ftm_lakehouse.storage.journal import get_journal

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()

# Test-mode switch:
#   testclient (default) – local fixtures + in-process FastAPI test API.
#   docker               – additionally run the ``docker`` param variants
#                          of parametrized fixtures against the running
#                          ``docker-compose`` stack (lakehouse + putfs +
#                          nginx).
# Fixture variants tagged with ``pytest.mark.docker`` (typically the
# ``"docker"`` ``pytest.param``) get auto-skipped when not in docker mode.
LAKEHOUSE_TEST_MODE = os.environ.get("LAKEHOUSE_TEST_MODE", "testclient")
LAKEHOUSE_TEST_URL = os.environ.get("LAKEHOUSE_TEST_URL", "http://127.0.0.1:8000")
# Host-side path of the bind-mounted ``./data`` volume the docker stack
# writes to. Tests in docker mode can inspect / assert on the on-disk
# layout the same way local-mode tests do via ``tmp_path``.
LAKEHOUSE_TEST_DATA_DIR = Path(
    os.environ.get("LAKEHOUSE_TEST_DATA_DIR", "./data")
).absolute()


def skip_unless_docker_mode() -> None:
    """Skip the calling test unless ``LAKEHOUSE_TEST_MODE=docker``.

    Use at the top of a docker fixture branch that builds its own
    upstream (not via :func:`make_docker_repo`) so a plain ``pytest``
    run (without ``make start``) doesn't try to reach a stack that
    isn't there.
    """
    if LAKEHOUSE_TEST_MODE != "docker":
        pytest.skip("docker stack not running (LAKEHOUSE_TEST_MODE != docker)")


def make_docker_dataset_name() -> str:
    """Unique dataset name per test so concurrent / repeated docker runs
    don't collide on the shared bind-mounted ``./data`` volume."""
    return f"e2e_{uuid.uuid4().hex[:8]}"


def docker_data_path(dataset_name: str) -> Path:
    """Host-side path of a dataset's directory under the docker bind mount.

    The compose stack mounts ``./data`` into every service at ``/data``,
    so a dataset's on-disk layout under ``./data/{name}`` is directly
    inspectable from the test process running on the host.
    """
    return LAKEHOUSE_TEST_DATA_DIR / dataset_name


def make_docker_repo(
    dataset_name: str | None = None,
) -> tuple[EntityRepository, Path]:
    """Build an EntityRepository pointing at the docker-compose stack.

    Skips the calling test when ``LAKEHOUSE_TEST_MODE != "docker"`` so a
    plain ``pytest`` run (without ``make start``) doesn't try to reach a
    stack that isn't there.

    Returns ``(repo, base_path)`` mirroring the local / api fixture
    shape: ``base_path`` is the host-side bind-mount directory the
    container writes into, so tests can assert on tag files and other
    on-disk artefacts the same way.

    The api-key / api-secret headers attached by
    :mod:`ftm_lakehouse.core.api` (driven by ``LAKEHOUSE_API_KEY`` /
    ``LAKEHOUSE_API_SECRET`` from ``[tool.pytest_env]``) authenticate
    through nginx automatically.
    """
    if LAKEHOUSE_TEST_MODE != "docker":
        pytest.skip("docker stack not running (LAKEHOUSE_TEST_MODE != docker)")
    name = dataset_name or make_docker_dataset_name()
    repo = EntityRepository(name, uri=f"{LAKEHOUSE_TEST_URL}/{name}")
    return repo, docker_data_path(name)


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
    factories.clear_caches()
    get_journal.cache_clear()
    get_lakehouse.cache_clear()
    yield
    # Clear after test
    factories.clear_caches()
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
# Avoid ``:8000`` so we don't collide with the docker compose stack's nginx
# when running tests in docker mode.
RANGE_HTTP_PORT = int(os.environ.get("LAKEHOUSE_TEST_RANGE_HTTP_PORT", "8765"))


def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "RangeHTTPServer", str(RANGE_HTTP_PORT)],
        cwd=str(FIXTURES_PATH),
    )
    while True:
        try:
            requests.get(f"http://localhost:{RANGE_HTTP_PORT}")
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
