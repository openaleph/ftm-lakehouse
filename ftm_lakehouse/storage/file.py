"""FileStore - JSON metadata file storage."""

from typing import Literal

from anystore.types import Uri

from ftm_lakehouse.model.file import File
from ftm_lakehouse.storage.base import ModelStorage


class FileStore(ModelStorage[File, Literal[True]]):
    """
    Metadata storage for `File` models.

    Stores `File` models as JSON files at specified keys. It raises when the
    requested key doesn't exist.
    """

    model = File

    def __init__(self, uri: Uri) -> None:
        super().__init__(uri)
        self.get = self._store.get
        self.iterate = self._store.iterate_values
        self.iterate_keys = self._store.iterate_keys

    def put(self, file: File) -> None:
        """Store a Pydantic model as JSON at the given key."""
        self._store.put(file.meta_path, file)

    def delete(self, file: File) -> None:
        """Delete metadata at the given key."""
        self._store.delete(file.meta_path)
