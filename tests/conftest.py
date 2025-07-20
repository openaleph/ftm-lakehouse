import subprocess
import sys
import time
from pathlib import Path

import boto3
import pytest
import requests
from anystore import get_store
from anystore.mirror import mirror
from moto.server import ThreadedMotoServer

from ftm_lakehouse.lake.base import (
    DatasetLakehouse,
    Lakehouse,
    get_dataset,
    get_lakehouse,
)

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="session")
def fixtures_path() -> Path:
    return FIXTURES_PATH


@pytest.fixture(scope="function")
def tmp_lake(tmp_path) -> Lakehouse:
    return get_lakehouse(tmp_path)


@pytest.fixture(scope="function")
def tmp_dataset(tmp_path) -> DatasetLakehouse:
    lake = get_lakehouse(tmp_path)
    return lake.get_dataset("tmp_dataset")


@pytest.fixture(autouse=True, scope="function")
def cache_clear():
    get_dataset.cache_clear()
    get_lakehouse.cache_clear()
    yield


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
@pytest.fixture(scope="session", autouse=True)
def moto_server():
    """Fixture to run a mocked AWS server for testing with some data buckets."""
    server = ThreadedMotoServer(port=8888)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    s3 = boto3.resource("s3", region_name="us-east-1", endpoint_url=endpoint)
    s3.create_bucket(Bucket="lakehouse")
    s3.create_bucket(Bucket="data")
    s3.create_bucket(Bucket="s3_dataset")
    from_store = get_store(uri=FIXTURES_PATH / "src", serialization_mode="raw")
    to_store = get_store(uri="s3://data", serialization_mode="raw")
    mirror(from_store, to_store, use_worker=False)
    yield endpoint
    server.stop()
