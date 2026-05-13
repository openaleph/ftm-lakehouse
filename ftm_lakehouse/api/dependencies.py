"""Shared FastAPI dependencies and constants for API routes."""

from typing import Annotated

from fastapi import Depends, Request

from ftm_lakehouse.dataset import Dataset as _Dataset
from ftm_lakehouse.storage.journal import BaseJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal
from ftm_lakehouse.util import validate_dataset_name


def get_dataset(dataset: str, request: Request) -> _Dataset:
    """Resolve a Dataset from the lakehouse via app state."""
    validate_dataset_name(dataset)
    return request.app.state.lake.get_dataset(dataset)


Dataset = Annotated[_Dataset, Depends(get_dataset)]


def get_journal(dataset: str) -> BaseJournalStore:
    """Get a JournalStore instance using settings-resolved URI."""
    validate_dataset_name(dataset)
    return _get_journal(dataset)


Journal = Annotated[BaseJournalStore, Depends(get_journal)]
