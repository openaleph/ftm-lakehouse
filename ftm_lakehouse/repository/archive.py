"""ArchiveRepository - file archive operations using BlobStore + FileStore (for
metadata) and optional TextStore for extracted fulltext."""

from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager

from anystore.store import get_store_for_uri
from anystore.store.base import BaseStore
from anystore.store.virtual import open_virtual
from anystore.types import BytesGenerator, Uri
from anystore.util import DEFAULT_HASH_ALGORITHM, join_relpaths, make_checksum
from banal import clean_dict

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model import File
from ftm_lakehouse.model.file import Files
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage import BlobStore, FileStore, TextStore
from ftm_lakehouse.storage.base import ByteStorage
from ftm_lakehouse.util import make_checksum_key


class ArchiveRepository(BaseRepository):
    """
    Repository for file archive operations.

    Combines BlobStore (raw bytes) and FileStore (JSON metadata)
    to provide content-addressed file storage.

    Blobs are stored once per checksum, but each unique source path
    creates its own metadata file (keyed by File.id).

    Optionally, extracted text (by different origins) can be stored and
    retrieved. As well, other programs can write arbitrary additional data to
    the archive (such as pdf page thumbnails).

    Example:
        ```python
        archive = ArchiveRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Archive a file
        file = archive.store("path/to/file.pdf")

        # Retrieve file info
        file = archive.get_file(checksum)

        # Stream file contents by checksum
        for chunk in archive.stream(file.checksum):
            process(chunk)
        ```
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        self._blobs = BlobStore(uri)
        self._files = FileStore(uri)
        self._txts = TextStore(uri)
        self._data = ByteStorage(uri)

    def exists(self, checksum: str) -> bool:
        """Check if blob exists for the given checksum."""
        return self._blobs.exists(checksum)

    def get_file(self, checksum: str, file_id: str | None = None) -> File:
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
        for file in self.get_all_files(checksum):
            return file
        raise FileNotFoundError(checksum)

    def get_all_files(self, checksum: str) -> Files:
        """
        Iterate all metadata files for the given checksum.

        Multiple crawlers may have archived the same file content from
        different source paths, each creating their own metadata file.
        """
        prefix = path.archive_prefix(checksum)
        yield from self._files.iterate(prefix, glob="*.json")

    def iterate_files(self) -> Files:
        """Iterate all file metadata in the archive."""
        yield from self._files.iterate(path.ARCHIVE, glob="**/*.json")

    def put_file(self, file: File) -> File:
        """Store file metadata object."""
        file.store = str(self.uri)
        file.dataset = self.dataset
        self._files.put(file)
        return file

    def stream(self, checksum: str) -> BytesGenerator:
        """Stream blob contents as bytes."""
        yield from self._blobs.stream(checksum)

    def open(self, checksum: str) -> ContextManager[IO[bytes]]:
        """Get a file-like handle for reading."""
        return self._blobs.open(checksum)

    def local_path(self, checksum: str) -> ContextManager[Path]:
        """
        Get the local path to the blob.

        If storage is local, returns actual path. Otherwise, creates
        a temporary local copy that is cleaned up after context exit.
        """
        return self._blobs.local_path(checksum)

    def store(
        self,
        uri: Uri,
        remote_store: BaseStore | None = None,
        file: File | None = None,
        checksum: str | None = None,
        **metadata: Any,
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
            **metadata: Additional data to store in file's extra field, including
                FollowTheMoney properties for the `Document` schema

        Returns:
            File metadata object
        """
        if remote_store is None:
            remote_store, uri = get_store_for_uri(uri)

        # store bytes blob
        checksum = self.store_blob(uri, remote_store, checksum)

        # file metadata
        if file is None:
            info = remote_store.info(uri)
            file = File.from_info(info, checksum)

        file.checksum = checksum

        for key in list(metadata.keys()):
            if key in file.__class__.model_fields:
                setattr(file, key, metadata.pop(key))
        file.extra = clean_dict(metadata)
        file.store = str(self.uri)
        file.dataset = self.dataset

        # Store metadata
        self._files.put(file)
        # Notify archive was updated
        self._tags.set(tag.ARCHIVE_UPDATED)

        self.log.info(
            f"Archived `{file.key} ({file.checksum})`",
            checksum=file.checksum,
        )

        return file

    def store_blob(
        self,
        uri: Uri,
        remote_store: BaseStore | None = None,
        checksum: str | None = None,
    ) -> str:
        """
        Store bytes blob from given uri if it doesn't exist yet.

        Args:
            uri: Local or remote URI to the file
            remote_store: Fetch the URI as key from this store
            checksum: Content hash (skip computation if provided)

        Returns:
            checksum
        """
        if checksum and self.exists(checksum):
            self.log.debug("Blob already exists, skipping", checksum=checksum)
            return checksum

        if remote_store is None:
            remote_store, uri = get_store_for_uri(uri)

        with open_virtual(
            uri,
            remote_store,
            checksum=DEFAULT_HASH_ALGORITHM if checksum is None else None,
        ) as fh:
            fh.checksum = checksum or fh.checksum
            if fh.checksum is None:
                raise RuntimeError(f"No checksum for `{uri}`")

            if self.exists(fh.checksum):
                self.log.debug("Blob already exists, skipping", checksum=fh.checksum)
                return fh.checksum

            # actually store the blob
            self.log.info(f"Storing blob `{fh.checksum}` ...", checksum=fh.checksum)
            self.write_blob(fh, checksum)

            return fh.checksum

    def write_blob(self, fh: BinaryIO, checksum: str | None = None) -> str:
        """Write a blob from the given open file-handler"""
        if checksum and self.exists(checksum):
            self.log.debug("Blob already exists, skipping", checksum=checksum)
            return checksum
        if not checksum:
            checksum = make_checksum(fh)
        with self._blobs.open(checksum, "wb") as out:
            while chunk := fh.read(8192):
                out.write(chunk)
        return checksum

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

    def put_data(self, checksum: str, path: str, data: bytes) -> None:
        """Store raw data at the given path"""
        key = join_relpaths(make_checksum_key(checksum), path)
        self._data._store.put(key, data)

    def get_data(self, checksum: str, path: str) -> bytes:
        """Get raw data at the given path"""
        key = join_relpaths(make_checksum_key(checksum), path)
        return self._data._store.get(key)
