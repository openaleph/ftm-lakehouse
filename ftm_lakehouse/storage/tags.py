"""TagStore - key-value freshness tracking."""

from datetime import datetime
from typing import Iterable, Literal

from anystore.store import BaseStore, get_store
from anystore.tags import Tags as AnyTags
from anystore.types import Uri
from anystore.util import join_uri

from ftm_lakehouse.core.conventions import path


class TagStore(AnyTags):
    """
    Key-value store for freshness tracking.

    Tags are timestamps stored as key-value pairs, used to track
    when resources were last updated and determine if processing
    is needed.

    Layout: tags/{tenant}/{key}

    This store has the "tags/{tenant}" key prefix set, so clients must use
    relative paths from there.
    """

    store = BaseStore[datetime, Literal[False]]

    def __init__(self, uri: Uri, tenant: str | None = None) -> None:
        store = get_store(
            uri=uri, raise_on_nonexist=False, key_prefix=path.tag(tenant=tenant)
        )
        super().__init__(store)

    def is_latest(self, key: str, dependencies: Iterable[str]) -> bool:
        """
        Check if the tag is more recent than all dependencies.

        Args:
            key: Tag key to check
            dependencies: Tag keys that this key depends on

        Returns:
            True if key is newer than all dependencies, False otherwise
        """
        last_updated = self.get(key)
        if last_updated is None:
            return False
        updated_dependencies = (i for i in map(self.get, dependencies) if i)
        return all(last_updated > i for i in updated_dependencies)

    def set(self, key: str, timestamp: datetime | None = None) -> datetime:
        """Set a tag to the given timestamp (or now if not provided)."""
        ts = timestamp or datetime.now()
        self.put(key, ts)
        return ts

    def __repr__(self) -> str:
        prefix = self.store.key_prefix or ""
        return f"<{self.__class__.__name__}({join_uri(self.store.uri, prefix)})>"
