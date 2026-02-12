from functools import cache

from anystore.logic.uri import UriHandler

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.storage.journal.api import ApiJournalStore, ApiJournalWriter
from ftm_lakehouse.storage.journal.base import (
    BaseJournalStore,
    BaseJournalWriter,
    JournalRow,
    JournalRows,
)
from ftm_lakehouse.storage.journal.sql import SqlJournalStore, SqlJournalWriter

# Default implementations
JournalStore = SqlJournalStore
JournalWriter = SqlJournalWriter


@cache
def get_journal(dataset: str, uri: str | None = None) -> BaseJournalStore:
    """Create journal store: ApiJournalStore for HTTP URIs, SqlJournalStore otherwise."""
    uri = uri or Settings().journal_uri
    handler = UriHandler(uri)
    if handler.is_http:
        return ApiJournalStore(dataset, uri)
    return SqlJournalStore(dataset, uri)


__all__ = [
    "BaseJournalStore",
    "BaseJournalWriter",
    "JournalRow",
    "JournalRows",
    "JournalStore",
    "JournalWriter",
    "SqlJournalStore",
    "SqlJournalWriter",
    "ApiJournalStore",
    "ApiJournalWriter",
    "get_journal",
]
