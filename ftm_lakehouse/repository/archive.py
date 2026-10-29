"""ArchiveRepository - file archive operations using content-addressed blob
storage, optional extracted fulltext and raw data."""

from datetime import datetime
from pathlib import Path
from typing import IO, BinaryIO, ContextManager, Generator

from anystore.io.read import open_virtual
from anystore.logic.constants import DEFAULT_MODE
from anystore.logic.io import stream
from anystore.store import get_store
from anystore.types import BytesGenerator, Uri
from anystore.util import (
    join_relpaths,
    make_checksum,
    mask_uri,
    validate_relative_uri,
)
from ftmq.store.base import DEFAULT_ORIGIN

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.util import validate_checksum


class ArchiveRepository(DatasetHandle):
    """
    Repository for file archive operations.

    Content-addressed blob storage (raw bytes): a blob is stored once per
    checksum.

    Optionally, extracted text (by different origins) can be stored and
    retrieved. As well, other programs can write arbitrary additional data to
    the archive (such as pdf page thumbnails or their own file metadata) via
    [`put_data`][ArchiveRepository.put_data].

    Example:
        ```python
        archive = ArchiveRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Archive a file
        checksum = archive.store("path/to/file.pdf")

        # Stream file contents by checksum
        for chunk in archive.stream(checksum):
            process(chunk)
        ```
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        self._txts = get_store(
            self._store_uri, serialization_mode="auto", raise_on_nonexist=False
        )

    def exists(self, checksum: str) -> bool:
        """Check if blob exists for the given checksum."""
        return self._store.exists(path.ArchiveKey(checksum).blob)

    def iterate_checksums(self) -> Generator[str, None, None]:
        """Iterate the checksums of all blobs in the archive."""
        # exactly the checksum depth – a `put_data` path may end in "blob" too
        glob = f"*/*/*/*/{path.ARCHIVE_BLOB}"
        for key in self._store.iterate_keys(prefix=path.ARCHIVE, glob=glob):
            yield key.split("/")[-2]

    def stream(self, checksum: str) -> BytesGenerator:
        """Stream blob contents as bytes."""
        yield from self._store.stream(path.ArchiveKey(checksum).blob)

    def open(self, checksum: str) -> ContextManager[IO[bytes]]:
        """Get a file-like handle for reading."""
        return self._store.open(path.ArchiveKey(checksum).blob, mode=DEFAULT_MODE)

    def to_uri(self, checksum: str) -> str:
        return self._store.to_uri(path.ArchiveKey(checksum).blob)

    def local_path(self, checksum: str) -> ContextManager[Path]:
        """
        Get the local path to the blob.

        If storage is local, returns actual path. Otherwise, creates
        a temporary local copy that is cleaned up after context exit.
        """
        return self._store.local_path(path.ArchiveKey(checksum).blob)

    def store(
        self, uri: Uri, checksum: str | None = None, tag_updated: bool = True
    ) -> str:
        """
        Archive a file from a local or remote URI.

        Args:
            uri: Local or remote URI to the file
            checksum: Content hash (skip computation if provided)
            tag_updated: Touch the `archive/last_updated` tag

        Returns:
            checksum
        """
        checksum = self.store_blob(uri, checksum)
        if tag_updated:
            # Notify archive was updated
            self.touch()
        self.log.info(f"Archived `{mask_uri(uri)}` ({checksum})", checksum=checksum)
        return checksum

    def store_blob(self, uri: Uri, checksum: str | None = None) -> str:
        """
        Store bytes blob from given uri if it doesn't exist yet.

        Args:
            uri: Local or remote URI to the file
            checksum: Content hash (skip computation if provided)

        Returns:
            checksum
        """
        if checksum:
            validate_checksum(checksum)
        if checksum and self.exists(checksum):
            self.log.debug("Blob already exists, skipping", checksum=checksum)
            return checksum

        with open_virtual(uri, algorithm=CHECKSUM_ALGORITHM) as fh:
            if self.exists(fh.checksum):
                self.log.debug("Blob already exists, skipping", checksum=fh.checksum)
                return fh.checksum

            self.log.info(f"Storing blob `{fh.checksum}` ...", checksum=fh.checksum)
            self.write_blob(fh, fh.checksum, check_exists=False)
            return fh.checksum

    def write_blob(
        self, fh: BinaryIO, checksum: str | None = None, check_exists: bool = True
    ) -> str:
        """Write a blob from the given open file-handler"""
        if check_exists and checksum and self.exists(checksum):
            self.log.debug("Blob already exists, skipping", checksum=checksum)
            return checksum
        if not checksum:
            checksum = make_checksum(fh, algorithm=CHECKSUM_ALGORITHM)
            if self.exists(checksum):
                self.log.debug("Blob already exists, skipping", checksum=checksum)
                return checksum
            fh.seek(0)
        with self._store.open(path.ArchiveKey(checksum).blob, "wb") as out:
            stream(fh, out)
        return checksum

    def put_txt(self, checksum: str, text: str, origin: str = DEFAULT_ORIGIN) -> None:
        """Store extracted text for a file.

        Raises:
            ValueError: If ``checksum`` is not a valid SHA256 hex digest or
                ``origin`` is not a safe path component
                (see `validate_origin`).
        """
        key = path.ArchiveKey(checksum).txt(origin)
        self._txts.put(key, text)

    def get_txt(self, checksum: str, origin: str | None = None) -> str | None:
        """Get extracted text for a file. If `origin`, get by this specific
        extraction, otherwise get the first txt value (no guaranteed order)"""
        if origin:
            key = path.ArchiveKey(checksum).txt(origin)
            return self._txts.get(key)
        for value in self._txts.iterate_values(
            prefix=path.ArchiveKey(checksum), glob="*.txt"
        ):
            return value

    def put_data(self, checksum: str, path: str, data: bytes) -> None:
        """Store raw data at the given path in the checksum's archive directory

        Raises:
            ValueError: If ``checksum`` is not a valid SHA256 hex digest or
                ``path`` is empty, absolute, has a ``..`` segment or is
                reserved (``blob`` or a top-level ``*.txt``).
        """
        self._store.put(self._data_key(checksum, path), data)

    def get_data(self, checksum: str, path: str) -> bytes:
        """Get raw data at the given path in the checksum's archive directory

        Raises:
            ValueError: If ``checksum`` is not a valid SHA256 hex digest or
                ``path`` is empty, absolute, has a ``..`` segment or is
                reserved (``blob`` or a top-level ``*.txt``).
        """
        return self._store.get(self._data_key(checksum, path))

    def _data_key(self, checksum: str, key: str) -> str:
        # out here because `put_data` / `get_data` shadow the `path` module
        relative = validate_relative_uri(key)
        if not relative:  # "." normalises to nothing
            raise ValueError(f"Invalid empty data path: `{key}`")
        # the file's content and what `get_txt` reads as extracted text
        if relative == path.ARCHIVE_BLOB or (
            "/" not in relative and relative.endswith(".txt")
        ):
            raise ValueError(f"Reserved data path: `{key}`")
        return join_relpaths(path.ArchiveKey(checksum), relative)

    def touch(self) -> datetime:
        with self._tags.touch(tag.ARCHIVE_UPDATED) as now:
            return now
