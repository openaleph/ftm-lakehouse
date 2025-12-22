"""ArchiveRepository - file archive operations using BlobStore + FileStore (for
metadata) and optional TextStore for extracted fulltext."""

from pathlib import Path
from typing import IO, Any, ContextManager, Generator

from anystore.store import get_store_for_uri
from anystore.store.base import BaseStore
from anystore.store.virtual import open_virtual
from anystore.types import BytesGenerator, Uri
from anystore.util import DEFAULT_HASH_ALGORITHM
from banal import clean_dict

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model import File
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage import BlobStore, FileStore, TextStore


class ArchiveRepository(BaseRepository):
    """
    Repository for file archive operations.

    Combines BlobStore (raw bytes) and FileStore (JSON metadata)
    to provide content-addressed file storage.

    Blobs are stored once per checksum, but each unique source path
    creates its own metadata file (keyed by File.id).

    Optionally, extracted text (by different origins) can be stored and
    retrieved.

    Example:
        ```python
        archive = ArchiveRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Archive a file
        file = archive.store("path/to/file.pdf")

        # Retrieve file info
        file = archive.get(checksum)

        # Stream file contents
        for chunk in archive.stream(file):
            process(chunk)
        ```
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        self._blobs = BlobStore(uri)
        self._files = FileStore(uri)
        self._txts = TextStore(uri)

    def exists(self, checksum: str) -> bool:
        """Check if blob exists for the given checksum."""
        return self._blobs.exists(checksum)

    def get(self, checksum: str, file_id: str | None = None) -> File:
        """
        Get file metadata for the given checksum.

        Args:
            checksum: SHA1 checksum of file
            file_id: Optional File.id to get specific metadata

        Raises:
            FileNotFoundError: When no metadata file exists
        """
        if file_id is not None:
            key = path.archive_meta(checksum, file_id)
            return self._files.get(key)

        # Return first found metadata
        for file in self.get_all(checksum):
            return file
        raise FileNotFoundError(checksum)

    def get_all(self, checksum: str) -> Generator[File, None, None]:
        """
        Iterate all metadata files for the given checksum.

        Multiple crawlers may have archived the same file content from
        different source paths, each creating their own metadata file.
        """
        prefix = path.archive_prefix(checksum)
        yield from self._files.iterate(prefix, glob="*.json")

    def iterate(self) -> Generator[File, None, None]:
        """Iterate all file metadata in the archive."""
        yield from self._files.iterate(path.ARCHIVE, glob="**/*.json")

    def stream(self, file: File) -> BytesGenerator:
        """Stream file contents as bytes."""
        yield from self._blobs.stream(file.checksum)

    def put(self, file: File) -> File:
        """Store file metadata object."""
        file.store = str(self.uri)
        file.dataset = self.dataset
        self._files.put(file)
        return file

    def open(self, file: File) -> ContextManager[IO[bytes]]:
        """Get a file handle for reading."""
        return self._blobs.open(file.checksum)

    def local_path(self, file: File) -> ContextManager[Path]:
        """
        Get the local path to the file.

        If storage is local, returns actual path. Otherwise, creates
        a temporary copy that is cleaned up after context exit.
        """
        return self._blobs.local_path(file.checksum)

    def store(
        self,
        uri: Uri,
        remote_store: BaseStore | None = None,
        file: File | None = None,
        checksum: str | None = None,
        **extra: Any,
    ) -> File:
        """
        Archive a file from a local or remote URI.

        The blob is stored once per checksum, but each unique source path
        creates its own metadata file (keyed by File.id).

        Args:
            uri: Local or remote URI to the file
            remote_store: Fetch the URI as key from this store
            file: Optional metadata file object to patch
            checksum: Content hash (skip computation if provided)
            **extra: Additional data to store in file's extra field, including
                FollowTheMoney properties for the `Document` schema

        Returns:
            File metadata object
        """
        if remote_store is None:
            remote_store, uri = get_store_for_uri(uri)

        store_blob = True
        with open_virtual(
            uri,
            remote_store,
            checksum=DEFAULT_HASH_ALGORITHM if checksum is None else None,
        ) as fh:
            fh.checksum = checksum or fh.checksum
            if fh.checksum is None:
                raise RuntimeError(f"No checksum for `{uri}`")

            if self.exists(fh.checksum):
                self.log.debug(
                    "Blob already exists, storing metadata only",
                    checksum=fh.checksum,
                )
                store_blob = False

            if file is None:
                info = remote_store.info(uri)
                file = File.from_info(info, fh.checksum)

            file.checksum = fh.checksum

            if store_blob:
                with self._blobs.open(fh.checksum, "wb") as out:
                    out.write(fh.read())

        # Set metadata
        file.extra = clean_dict(extra)
        file.store = str(self.uri)
        file.dataset = self.dataset

        # Store metadata
        self._files.put(file)
        # Notify archive was updated
        self._tags.set(tag.ARCHIVE_UPDATED)

        self.log.info(
            f"Archived `{file.key} ({file.checksum})`",
            checksum=file.checksum,
            stored_blob=store_blob,
        )

        return file

    def delete(self, file: File) -> None:
        """
        Delete a file's metadata from the archive.

        The blob is never deleted. (FIXME)
        """
        self.log.warning(
            "Deleting file metadata",
            checksum=file.checksum,
            file_id=file.id,
        )
        self._files.delete(file)

    def put_txt(self, checksum: str, text: str, origin: str = "default") -> None:
        """Store extracted text for a file."""
        self._txts.put(checksum, text, origin)

    def get_txt(self, checksum: str, origin: str | None = None) -> str | None:
        """Get extracted text for a file. If `origin`, get by this specific
        extraction, otherwise get the first txt value (no guaranteed order)"""
        return self._txts.get(checksum, origin)
