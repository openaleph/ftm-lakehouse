"""QueueStore - CRUD action queue for ordered mutation log."""

from typing import Type

from anystore.interface.queue import Queue
from anystore.store import get_store
from anystore.types import M, Uri
from anystore.util import join_uri

from ftm_lakehouse.core.conventions import path


class QueueStore(Queue):
    """
    CRUD action queue for ordered mutation log.

    All mutations (entity upsert/delete, file archive, mapping updates)
    go through this queue, ordered by UUID7 timestamp.

    Layout: queue/{tenant}/{uuid7}.json

    This store has the "queue/{tenant}" key prefix set, so clients must use
    relative paths from there.
    """

    def __init__(self, uri: Uri, model: Type[M], tenant: str | None = None) -> None:
        uri = join_uri(uri, path.queue(tenant))
        store = get_store(uri)
        super().__init__(store, model)
