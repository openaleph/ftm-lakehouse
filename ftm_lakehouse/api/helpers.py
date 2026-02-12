"""Shared FastAPI dependencies and constants for API routes."""

from typing import Annotated

from fastapi import Depends, Request

from ftm_lakehouse.dataset import Dataset as _Dataset
from ftm_lakehouse.storage.journal import BaseJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal

NDJSON_CONTENT_TYPE = "application/x-ndjson"


def get_dataset(dataset: str, request: Request) -> _Dataset:
    """Resolve a Dataset from the lakehouse via app state."""
    return request.app.state.lake.get_dataset(dataset)


Dataset = Annotated[_Dataset, Depends(get_dataset)]


def get_journal(dataset: str, request: Request) -> BaseJournalStore:
    """Get a JournalStore instance using the configured URI."""
    journal_uri = request.app.state.journal_uri
    return _get_journal(dataset, journal_uri)


Journal = Annotated[BaseJournalStore, Depends(get_journal)]
