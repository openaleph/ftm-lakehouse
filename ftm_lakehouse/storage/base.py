from typing import Generic

from anystore.serialize import Mode
from anystore.store import BaseStore
from anystore.types import M, Raise, Uri, V


class BaseStorage(Generic[V, Raise]):
    """
    Base storage class for file-like anystore backend with configurable
    serialization mode and handling of non-existing items.
    """

    serialization_mode: Mode | None = None
    raise_on_nonexist: bool | None = True
    _store: BaseStore[V, Raise]

    def __init__(self, uri: Uri) -> None:
        self.uri = uri
        self._store = BaseStore[V, Raise](
            uri=uri,
            serialization_mode=self.serialization_mode,
            raise_on_nonexist=self.raise_on_nonexist,
        )


class ByteStorage(BaseStorage[bytes, Raise]):
    serialization_mode = "raw"
    _store: BaseStore[bytes, Raise]


class StrStorage(BaseStorage[str, Raise]):
    serialization_mode = "auto"
    _store: BaseStore[str, Raise]


class ModelStorage(BaseStorage[M, Raise]):
    """
    Storage class with a guaranteed pydantic model for serialization.
    """

    model: type[M]
    _store: BaseStore[M, Raise]

    def __init__(self, uri: Uri) -> None:
        self.uri = uri
        self._store = BaseStore[M, Raise](
            uri=uri,
            serialization_mode=self.serialization_mode,
            model=self.model,
            raise_on_nonexist=self.raise_on_nonexist,
        )


DEFAULT_ORIGIN = "default"
