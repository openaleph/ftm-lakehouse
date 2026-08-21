from functools import cache

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.storage.journal.api import ApiJournalStore, ApiJournalWriter
from ftm_lakehouse.storage.journal.base import (
    BaseJournalStore,
    BaseJournalWriter,
    RecordBatches,
    StatementTables,
)
from ftm_lakehouse.storage.journal.sql import (
    PostgresJournalStore,
    SqliteJournalStore,
    SqlJournalStore,
    SqlJournalWriter,
    sql_journal,
)


@cache
def get_journal(dataset: str, uri: str | None = None) -> BaseJournalStore:
    """Create journal store: ApiJournalStore for HTTP URIs, SqlJournalStore otherwise."""
    settings = Settings()
    uri = uri or settings.resolved_journal_uri
    if settings.api_mode:
        return ApiJournalStore(dataset, uri)
    return sql_journal(dataset, uri)


__all__ = [
    "BaseJournalStore",
    "BaseJournalWriter",
    "RecordBatches",
    "StatementTables",
    "SqlJournalStore",
    "SqlJournalWriter",
    "SqliteJournalStore",
    "PostgresJournalStore",
    "sql_journal",
    "ApiJournalStore",
    "ApiJournalWriter",
    "get_journal",
]
