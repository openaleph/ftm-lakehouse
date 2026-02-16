from functools import cache

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
    settings = Settings()
    uri = uri or settings.resolved_journal_uri
    if settings.api_mode:
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
