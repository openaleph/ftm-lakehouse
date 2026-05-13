"""Shared FastAPI dependencies and constants for API routes."""

from typing import Annotated

from fastapi import Body, Depends, Request
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ftm_lakehouse.core.settings import ApiSettings
from ftm_lakehouse.dataset import Dataset as _Dataset
from ftm_lakehouse.storage.journal import BaseJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal
from ftm_lakehouse.util import validate_dataset_name

api_settings = ApiSettings()

EMBED = Body(embed=True)
"""Use for single-parameter endpoints so FastAPI expects ``{"<name>": value}``
rather than the bare value as the entire body."""


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


class QueryBody(BaseModel):
    """Pydantic model for ``entities`` / ``statements`` query bodies.

    Pydantic enforces the type of ``entity_ids`` (must be a list of
    strings); validators enforce the runtime caps from
    :class:`ApiSettings`. Unknown fields pass through to ``ftmq.Query``
    as filter kwargs (``extra="allow"``).

    Use as ``body: QueryBody`` in a route signature; FastAPI parses and
    validates the request body against this model before the handler
    runs, returning a 422 (Pydantic's standard error shape) on any
    violation.
    """

    model_config = ConfigDict(extra="allow")

    entity_ids: list[str] | None = None
    flush_first: bool = False

    @model_validator(mode="before")
    @classmethod
    def _cap_filter_keys(cls, data: dict) -> dict:
        if isinstance(data, dict) and len(data) > api_settings.max_filter_keys:
            raise ValueError(
                f"query has {len(data)} filter keys; "
                f"maximum is {api_settings.max_filter_keys}"
            )
        return data

    @field_validator("entity_ids")
    @classmethod
    def _cap_entity_ids(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and len(v) > api_settings.max_entity_ids:
            raise ValueError(
                f"entity_ids length {len(v)} exceeds maximum of "
                f"{api_settings.max_entity_ids}"
            )
        return v

    def filter_kwargs(self) -> dict:
        """Filter kwargs to hand to ``ftmq.Query().where(...)`` – the
        caller-supplied extras only, with the two route-meta fields
        (``entity_ids``, ``flush_first``) excluded."""
        return self.model_dump(
            exclude={"entity_ids", "flush_first"}, exclude_unset=True
        )
