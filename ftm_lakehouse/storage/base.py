from typing import IO, ContextManager, Generic

from anystore.logic.serialize import Mode
from anystore.store import Store, get_store
from anystore.types import M, Raise, Uri, V
from anystore.util import ensure_uri

from ftm_lakehouse.exceptions import ImproperlyConfigured


class BaseStorage(Generic[V, Raise]):
    """
    Base storage class for file-like anystore backend with configurable
    serialization mode and handling of non-existing items.
    """

    serialization_mode: Mode | None = None
    raise_on_nonexist: bool | None = True
    _store: Store[V, Raise]

    def __init__(self, uri: Uri) -> None:
        self.uri = ensure_uri(uri)
        self._store: Store[V, Raise] = get_store(
            uri=uri,
            serialization_mode=self.serialization_mode,
            raise_on_nonexist=self.raise_on_nonexist,
        )

    def to_uri(self, key: Uri) -> str:
        return self._store.to_uri(key)

    def exists(self, key: Uri) -> bool:
        return self._store.exists(key)

    def open(self, key: Uri, *args, **kwargs) -> ContextManager[IO]:
        return self._store.open(key, *args, **kwargs)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.uri})>"


class ByteStorage(BaseStorage[bytes, Raise]):
    serialization_mode = "raw"
    _store: Store[bytes, Raise]


class StrStorage(BaseStorage[str, Raise]):
    serialization_mode = "auto"
    _store: Store[str, Raise]


class ModelStorage(BaseStorage[M, Raise]):
    """
    Storage class with a guaranteed pydantic model for serialization.
    """

    model: type[M]
    _store: Store[M, Raise]

    def __init__(
        self,
        uri: Uri,
        model: type[M] | None = None,
        raise_on_nonexist: bool | None = None,
        **kwargs,
    ) -> None:
        self.uri = uri
        resolved_model = getattr(self, "model", None) or model
        if resolved_model is None:
            raise ImproperlyConfigured(
                "Must specify model class for `ModelStorage`, not None!"
            )
        self.model = resolved_model
        if raise_on_nonexist is not None:
            self.raise_on_nonexist = raise_on_nonexist
        self._store = get_store(
            uri=uri,
            serialization_mode=self.serialization_mode,
            model=self.model,
            raise_on_nonexist=self.raise_on_nonexist,
            **kwargs,
        )


DEFAULT_ORIGIN = "default"
