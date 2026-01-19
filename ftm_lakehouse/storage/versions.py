"""VersionStore - timestamped snapshot storage."""

from pathlib import Path
from typing import Generic

from anystore.mixins import BaseModel
from anystore.types import M, Uri
from anystore.util import join_relpaths

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.helpers.serialization import dump_model, load_model
from ftm_lakehouse.storage.base import ByteStorage
from ftm_lakehouse.storage.tags import TagStore


class VersionedModelStore(ByteStorage, Generic[M]):
    """
    Timestamped snapshot storage for a given model type.

    Stores versioned copies of serialized pydantic models in a snapshot
    directory (versions/YYYY/MM/timestamp/filename) while also writing
    to the main path.

    Layout (relative):
    .../filename
        - Main: {filename}
        - Version: versions/YYYY/MM/{timestamp}/{filename}
    """

    model: type[M]

    def __init__(self, uri: Uri, model: type[M]) -> None:
        super().__init__(uri)
        self._tags = TagStore(uri)
        self.model = model
        self.delete = self._store.delete
        self.exists = self._store.exists
        self.iterate_keys = self._store.iterate_keys

    def make(self, key: Uri, data: M) -> str:
        """
        Write obj to key and create a versioned snapshot.

        Args:
            key: Main storage key
            data: Pydantic model to store

        Returns:
            Path to the versioned copy
        """
        with self._tags.touch(key):
            key_path = Path(key)
            versioned_path = join_relpaths(
                key_path.parent.name, path.version(key_path.name)
            )
            raw = dump_model(key, data)
            self._store.put(versioned_path, raw)
            self._store.put(key, raw)
            return versioned_path

    def get(self, key: str) -> M:
        """Get the current version of a file."""
        return load_model(key, self._store.get(key), model=self.model)

    def list_versions(self, key: str) -> list[str]:
        """
        List all versioned copies of a file.

        Returns:
            List of version paths, sorted by timestamp
        """
        versions = []
        prefix = "versions"
        for version_key in self._store.iterate_keys(prefix=prefix):
            if version_key.endswith(key):
                versions.append(version_key)
        return sorted(versions)


class VersionStore(ByteStorage):
    def __init__(self, uri: Uri) -> None:
        super().__init__(uri)
        self.versions: dict[str, VersionedModelStore] = {}
        self.exists = self._store.exists
        self.get = self._store.get

    def make(self, key: str, obj: BaseModel) -> str:
        clz = obj.__class__.__name__
        if clz not in self.versions:
            self.versions[clz] = VersionedModelStore(self.uri, obj.__class__)
        return self.versions[clz].make(key, obj)
