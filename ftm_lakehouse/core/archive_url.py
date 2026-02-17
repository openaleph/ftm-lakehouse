"""
Resolve a fetchable URL for an archive blob based on storage backend.

Resolution priority (first match wins):
1. Public prefix configured (model or settings) -> join prefix + archive path
2. Cloud storage (S3/GCS/Azure/etc.) -> presigned URL via fsspec sign()
3. HTTP API mode -> build API URL + scoped JWT query param
4. Local filesystem -> return file:/// URI
"""

from anystore.store import Store
from anystore.util import join_uri

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings

settings = Settings()


def resolve_archive_url(
    store: Store,
    dataset: str,
    checksum: str,
    public_prefix: str | None = None,
) -> str:
    """Resolve a fetchable URL for an archive blob.

    Args:
        store: The dataset's anystore Store instance.
        dataset: Dataset name (used for API path scoping).
        checksum: SHA256 checksum identifying the blob.
        public_prefix: Optional public URL prefix (e.g. CDN base URL).

    Returns:
        A URL string suitable for HTTP clients or file access.
    """
    blob_path = path.archive_blob(checksum)

    # 1. Public prefix (CDN / reverse proxy)
    if public_prefix:
        return join_uri(public_prefix, blob_path)

    # 2. Cloud storage (S3, GCS, Azure, etc.) -> presigned URL
    try:
        full_path = f"{store.path}/{blob_path}"
        return store._fs.sign(full_path, expiration=settings.archive_url_expire)
    except NotImplementedError:
        pass

    # 3. HTTP API mode -> scoped JWT token
    if store.is_http:
        from ftm_lakehouse.api.auth import create_access_token

        base_url = str(store.uri).rstrip("/")
        resource_path = f"/{dataset}/{blob_path}"
        token = create_access_token(
            methods=["GET", "HEAD"],
            prefixes=[resource_path],
            exp=settings.archive_url_expire // 60 or 1,
        )
        return f"{base_url}/{dataset}/{blob_path}?token={token}"

    # 4. Local filesystem -> file:/// URI
    return store.to_uri(blob_path)
