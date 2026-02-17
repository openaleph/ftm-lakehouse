import jwt
import pytest
from anystore.store import get_store
from moto import mock_aws

from ftm_lakehouse.core.archive_url import resolve_archive_url
from ftm_lakehouse.core.conventions import path

CHECKSUM = "a" * 64
BLOB_PATH = path.archive_blob(CHECKSUM)
DATASET = "test_dataset"


class TestPublicPrefix:
    def test_returns_prefix_joined_with_blob_path(self, tmp_path):
        store = get_store(tmp_path / DATASET)
        url = resolve_archive_url(
            store, DATASET, CHECKSUM, public_prefix="https://cdn.example.com"
        )
        assert url == f"https://cdn.example.com/{BLOB_PATH}"

    def test_prefix_takes_priority_over_s3(self, moto_server):
        with mock_aws():
            moto_server.create_bucket(Bucket="test-bucket")
            store = get_store("s3://test-bucket/lake")
            url = resolve_archive_url(
                store, DATASET, CHECKSUM, public_prefix="https://cdn.example.com"
            )
            assert url == f"https://cdn.example.com/{BLOB_PATH}"


class TestCloudSign:
    def test_s3_returns_presigned_url(self, moto_server):
        with mock_aws():
            moto_server.create_bucket(Bucket="test-bucket")
            store = get_store("s3://test-bucket/lake")
            url = resolve_archive_url(store, DATASET, CHECKSUM)
            assert BLOB_PATH in url
            # Accept both SigV4 (X-Amz-Expires) and SigV2 (Expires) formats
            assert "X-Amz-Expires=" in url or "Expires=" in url
            assert "X-Amz-Signature=" in url or "Signature=" in url


class TestHttpApi:
    def test_returns_url_with_jwt_token(self):
        store = get_store("https://api.example.com/lakehouse")
        url = resolve_archive_url(store, DATASET, CHECKSUM)
        base, query = url.split("?", 1)
        assert base == f"https://api.example.com/lakehouse/{DATASET}/{BLOB_PATH}"
        assert query.startswith("token=")

    def test_jwt_has_correct_scopes(self):
        store = get_store("https://api.example.com/lakehouse")
        url = resolve_archive_url(store, DATASET, CHECKSUM)
        token = url.split("token=")[1]
        payload = jwt.decode(token, options={"verify_signature": False})
        assert payload["methods"] == ["GET", "HEAD"]
        assert len(payload["prefixes"]) == 1
        assert BLOB_PATH in payload["prefixes"][0]
        assert payload["prefixes"][0].startswith(f"/{DATASET}/")


class TestLocalFilesystem:
    def test_returns_file_uri(self, tmp_path):
        store = get_store(tmp_path / DATASET)
        url = resolve_archive_url(store, DATASET, CHECKSUM)
        assert url == f"file://{tmp_path}/{DATASET}/{BLOB_PATH}"
