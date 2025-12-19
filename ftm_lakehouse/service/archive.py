from pathlib import Path
from typing import IO, Any, ContextManager

from anystore.store import get_store_for_uri
from anystore.store.base import BaseStore
from anystore.store.virtual import get_virtual_path, open_virtual
from anystore.types import BytesGenerator, Uri
from anystore.util import DEFAULT_HASH_ALGORITHM
from banal import clean_dict
from ftmq.store.lake import DEFAULT_ORIGIN

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.core.decorators import touch
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.model import File, Files


class DatasetArchive(LakeMixin):
    def exists(self, checksum: str) -> bool:
        """Check if the given checksum exists as a file blob"""
        key = path.file_path_meta(checksum)
        return self.storage.exists(key)

    def lookup_file(self, checksum: str) -> File | None:
        """Get the file metadata for the given checksum"""
        key = path.file_path_meta(checksum)
        try:
            return self.storage.get(key, model=File)
        except FileNotFoundError:
            return

    def stream_file(self, file: File) -> BytesGenerator:
        """Stream the given file contents as a bytes line stream"""
        yield from self.storage.stream(file.archive_path)

    def open_file(self, file: File) -> ContextManager[IO[bytes]]:
        """Get an open file-handler for the opened file. It is closed after
        leaving the context"""
        return self.storage.open(file.archive_path)

    def local_path(self, file: File) -> ContextManager[Path]:
        """
        Get the (temporary) local path for the file. If the archive is on
        the local filesystem, the actual path will be used. Otherwise, a
        temporary copy from the remote archive is used and cleaned up after
        leaving the context.

        !!! warning
            Never delete or alter the file found at this path if the archive is
            local, as it is the original file path and not a temporary copy.
        """
        return get_virtual_path(file.archive_path, self.storage)

    def iter_files(self) -> Files:
        """Iterate through all metadata for files"""
        yield from self.storage.iterate_values(
            prefix=path.ARCHIVE, glob="**/*.json", model=File
        )

    @touch(tag.ARCHIVE_UPDATED)
    def archive_file(
        self,
        uri: Uri,
        remote_store: BaseStore | None = None,
        file: File | None = None,
        checksum: str | None = None,
        **data: Any,
    ) -> File:
        """
        Add the given path to the archive. This doesn't check for existing
        files (just overwrites them, capture that in higher logic).

        Args:
            uri: Local or remote uri to the file
            remote_store: Fetch the uri as key from this store
            file: Optional metadata file obj to patch
            checksum: Content hash (don't compute again)
            data: Optional data to store in file obj `raw` field
        """
        if remote_store is None:
            remote_store, uri = get_store_for_uri(uri)

        store = True
        with open_virtual(
            uri,
            remote_store,
            checksum=DEFAULT_HASH_ALGORITHM if checksum is None else None,
        ) as i:
            i.checksum = checksum or i.checksum
            if i.checksum is None:
                raise RuntimeError(f"No checksum for `{uri}`")
            if self.lookup_file(i.checksum) is not None:
                self.log.info(
                    "Source file already existing, updating metadata only",
                    checksum=i.checksum,
                    from_uri=uri,
                    to_store=self.storage.uri,
                )
                store = False
            if file is None:
                info = remote_store.info(uri)
                file = File.from_info(info, i.checksum, **data)
            # ensure checksum on file metadata
            file.checksum = i.checksum

            if store:  # skip if we already have that source
                with self.storage.open(file.archive_path, mode="wb") as o:
                    o.write(i.read())

        # extra data
        file.extra = clean_dict(data)

        # store metadata
        file_info = self.archive_file_info(file)

        self.log.info(
            f"Archived `{file.key} ({file.checksum})`",
            checksum=file.checksum,
            from_uri=uri,
            to_store=self.storage.uri,
            updated_existing=not store,
        )
        return file_info

    @touch(tag.ARCHIVE_UPDATED)
    def archive_file_info(self, file: File) -> File:
        # ensure correct metadata
        file.store = str(self.storage.uri)
        file.dataset = self.name
        self.storage.put(file.archive_path_meta, file, model=File)
        return file

    @touch(tag.ARCHIVE_UPDATED)
    def delete_file(self, file: File) -> None:
        """Delete the given file and its metadata from the storage"""
        self.log.warn("Deleting file from archive ...", checksum=file.checksum)
        self.storage.delete(file.archive_path_meta)
        self.storage.delete(file.archive_path)
        raise NotImplementedError("Delete file entity from statement store")

    def put_text(
        self, checksum: str, text: str, origin: str | None = DEFAULT_ORIGIN
    ) -> None:
        """Store extracted text for the given file checksum"""
        origin = origin or DEFAULT_ORIGIN
        key = f"{path.file_path(checksum)}.{origin}.txt"
        self.storage.put(key, text)
