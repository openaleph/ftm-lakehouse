"""BlobStore - raw file bytes storage (content-addressed)."""

from pathlib import Path
from typing import IO, ContextManager, Literal

from anystore.io import DEFAULT_MODE
from anystore.store.virtual import get_virtual_path
from anystore.types import BytesGenerator

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.storage.base import ByteStorage


class BlobStore(ByteStorage[Literal[True]]):
    """
    Content-addressed blob storage for raw file bytes.

    Blobs are stored once per checksum (SHA1). Multiple metadata files can
    reference the same blob. The Store will raise an error if the requested blob
    doesn't exist.

    Hashes are stored in a 3-part prefixed way.

    Path to a blob: archive/ab/cd/ef/abcdef1234.../data
    """

    serialization_mode = "raw"
    raise_on_nonexist = True

    def _blob_path(self, checksum: str) -> str:
        """Get the storage path for a blob."""
        return path.archive_blob(checksum)

    def exists(self, checksum: str) -> bool:
        """Check if blob exists for the given checksum."""
        return self._store.exists(self._blob_path(checksum))

    def put(self, checksum: str, data: bytes) -> None:
        """Store blob bytes for the given checksum."""
        self._store.put(self._blob_path(checksum), data)

    def get(self, checksum: str) -> bytes:
        """Retrieve blob bytes for the given checksum."""
        return self._store.get(self._blob_path(checksum))

    def stream(self, checksum: str) -> BytesGenerator:
        """Stream blob bytes for the given checksum."""
        yield from self._store.stream(self._blob_path(checksum))

    def open(
        self, checksum: str, mode: Literal["rb", "wb"] | None = None
    ) -> ContextManager[IO[bytes]]:
        """Get a file handle for the blob."""
        return self._store.open(self._blob_path(checksum), mode=mode or DEFAULT_MODE)

    def local_path(self, checksum: str) -> ContextManager[Path]:
        """
        Get the local path to the blob.

        If storage is local, returns actual path. Otherwise, creates
        a temporary copy that is cleaned up after context exit.

        !!! warning
            If the blob storage is local, this returns the actual file path. Do
            not modify or delete the file at this path.
        """
        return get_virtual_path(self._blob_path(checksum), self._store)

    def delete(self, checksum: str) -> None:
        """Delete the blob for the given checksum."""
        self._store.delete(self._blob_path(checksum))
