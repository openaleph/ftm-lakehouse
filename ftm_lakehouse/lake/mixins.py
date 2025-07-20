from functools import cached_property
from typing import Iterable

from anystore.logging import BoundLogger, get_logger
from anystore.model import BaseModel
from anystore.store import get_store
from anystore.store.fs import Store
from anystore.tags import Tags as AnyTags
from anystore.types import Uri
from anystore.util import join_relpaths, join_uri

from ftm_lakehouse.conventions import path
from ftm_lakehouse.util import dump_model


class Tags(AnyTags):
    """Tags interface"""

    def is_latest(self, key: str, dependencies: Iterable[str]) -> bool:
        """Return if the tag `key` is the most recent version compared to
        `dependencies`"""
        last_updated = self.get(key)
        if last_updated is None:
            return False
        updated_dependencies = (i for i in map(self.get, dependencies) if i)
        return all(last_updated > i for i in updated_dependencies)


class Versions:
    """Versions interface"""

    def __init__(self, storage: Store, tags: Tags) -> None:
        self.storage = storage
        self.tags = tags

    def make(self, key: Uri, obj: BaseModel) -> str:
        """Write `obj` to `path` and store a time-based version of it (`uuid7`)
        in the versions subfolder"""
        versioned_path = path.version(str(key))
        data = dump_model(key, obj)
        self.storage.put(versioned_path, data)
        self.storage.put(key, data)
        self.tags.touch(key)
        log = getattr(self, "log", None)
        if log:
            log.info(f"Update `{key}`", version=versioned_path, key=key)
        return versioned_path


class NamedUri:
    name: str
    uri: Uri


class LogMixin(NamedUri):
    @cached_property
    def log(self) -> BoundLogger:
        """Get a struct logger with prepopulated context"""
        name = f"ftm_lakehouse.{self.__class__.__name__}.{self.name}"
        return get_logger(name, dataset=self.name, storage=self.uri)


class CacheMixin(NamedUri):
    @cached_property
    def cache(self) -> Store:
        """
        Get the persistent cache (within the lakehouse storage)

        The cache will live in:
        ./lake/[dataset]/.cache/ftm_lakehouse
        """
        uri = join_uri(self.uri, ".cache/ftm_lakehouse")
        store = get_store(uri, raise_on_nonexist=False)
        assert isinstance(store, Store)
        return store

    def make_cache_key(self, key: Uri | None = None) -> str | None:
        """Return a cache key with a prefix for current class"""
        if key:
            return join_relpaths(self.__class__.__name__, key)


class StorageMixin(CacheMixin):
    @cached_property
    def storage(self) -> Store:
        """Get the storage"""
        store = get_store(uri=self.uri, serialization_mode="raw")
        assert isinstance(store, Store), f"Invalid storage: `{store.__class__}`"
        store.serialization_mode = "raw"
        return store

    @cached_property
    def versions(self) -> Versions:
        """Get the versions storage interface"""
        return Versions(self.storage, self.tags)

    @cached_property
    def tags(self) -> Tags:
        """Get the tags interface"""
        return Tags(self.cache)


class LakeMixin(LogMixin, StorageMixin):
    def __init__(self, name: str, uri: Uri, *args, **kwargs) -> None:
        self.name = name
        self.uri = uri
