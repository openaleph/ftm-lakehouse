"""VersionStore - timestamped snapshot storage."""

from typing import Any, Generator, Generic

from anystore.exceptions import DoesNotExist
from anystore.model.base import BaseModel
from anystore.store import get_store
from anystore.types import M, Uri

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.helpers.serialization import dump_model, load_model
from ftm_lakehouse.storage.tags import TagStore


class VersionedModelStore(Generic[M]):
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
        self.uri = uri
        self._store = get_store(uri, serialization_mode="raw")
        self._tags = TagStore(uri)
        self.model = model

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
            versioned_path = path.version(str(key))
            raw = dump_model(key, data)
            self._store.put(versioned_path, raw)
            self._store.put(key, raw)
            return versioned_path

    def get(self, key: str) -> M:
        """Get the current version of a file."""
        return load_model(key, self._store.get(key), model=self.model)

    def exists(self, key: str) -> bool:
        return self._store.exists(key)

    def delete(self, key: str) -> None:
        self._store.delete(key)

    def iterate_keys(self, **kwargs: Any) -> Generator[str, None, None]:
        yield from self._store.iterate_keys(**kwargs)

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


class VersionStore:
    def __init__(self, uri: Uri) -> None:
        self.uri = uri
        self._store = get_store(uri, serialization_mode="raw")
        self.versions: dict[str, VersionedModelStore] = {}

    def exists(self, key: str) -> bool:
        return self._store.exists(key)

    def make(self, key: str, obj: BaseModel) -> str:
        clz = obj.__class__.__name__
        if clz not in self.versions:
            self.versions[clz] = VersionedModelStore(self.uri, obj.__class__)
        return self.versions[clz].make(key, obj)

    def get(
        self, key: str, model: type[M], raise_on_nonexist: bool | None = True
    ) -> M | None:
        clz = model.__name__
        if clz not in self.versions:
            self.versions[clz] = VersionedModelStore(self.uri, model)
        try:
            return self.versions[clz].get(key)
        except DoesNotExist as e:
            if raise_on_nonexist:
                raise e
