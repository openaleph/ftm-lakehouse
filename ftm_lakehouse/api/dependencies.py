"""Shared FastAPI dependencies and constants for API routes."""

from typing import Annotated

from fastapi import Body, Depends, Request
from ftmq.query import Query
from pydantic import BaseModel, ConfigDict, field_validator
from pyrql import RQLError

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

    ``query`` carries the filter tree as an :meth:`RQL <ftmq.Query.to_rql>`
    string; ``order_by`` / ``limit`` / ``offset`` ride as sibling fields
    because RQL does not serialize them; ``origin`` scopes reads to one
    storage origin. Pydantic enforces the type of ``entity_ids`` (a list of
    strings) and rejects unknown keys (``extra="forbid"`` – the legacy flat
    filter-kwargs format fails loudly with a 422 instead of silently
    streaming an unfiltered result); :meth:`to_query` enforces the runtime
    complexity caps from :class:`ApiSettings` on the parsed query.

    Use as ``body: QueryBody`` in a route signature; FastAPI parses and
    validates the request body against this model before the handler
    runs, returning a 422 (Pydantic's standard error shape) on any
    violation.
    """

    model_config = ConfigDict(extra="forbid")

    flush_first: bool = False
    origin: str | None = None
    query: str | None = None
    order_by: list[str] | None = None
    limit: int | None = None
    offset: int | None = None

    @field_validator("query")
    @classmethod
    def validate_query_body(cls, v: str | None) -> str | None:
        """Enforce the API complexity caps on a parsed query.

        The RQL string is opaque to Pydantic, so the semantic DoS caps guard
        here: total filter-leaf count vs ``max_filter_keys`` and every ``in`` /
        ``not_in`` value list vs ``max_entity_ids`` – no request can smuggle an
        unbounded ``IN`` literal (which DuckDB chokes on) or filter fan-out past
        the boundary by encoding it as RQL instead of body fields.

        Raises:
            ValueError: When a cap is exceeded (mapped to a 422 by the app's
                exception handler).
        """
        if not v:
            return v
        try:
            q = Query.from_rql(v)
        except RQLError as e:
            raise ValueError(f"Invalid RQL query: {e}")
        leaves = list(q.q.iter_leaves()) if q.q else []
        if len(leaves) > api_settings.query_max_filter_keys:
            raise ValueError(
                f"query has {len(leaves)} filter conditions; "
                f"maximum is {api_settings.query_max_filter_keys}"
            )
        for leaf in leaves:
            if str(leaf.comparator) in ("in", "not_in"):
                if len(leaf.value) > api_settings.query_max_in_values:
                    raise ValueError(
                        f"`{leaf.key}__{leaf.comparator}` has {len(leaf.value)} "
                        f"values; maximum is {api_settings.query_max_in_values}"
                    )
        return v

    def to_query(self) -> Query | None:
        """Build the ftmq ``Query`` from the RQL string + sort / slice fields.

        Mirrors :func:`ftm_lakehouse.repository.entities.api._serialize_query`
        on the client side. Returns ``None`` for an empty body (an unfiltered,
        unsliced read).

        Raises:
            ValueError: On malformed RQL (pyrql's ``RQLError`` is re-raised
                as ``ValueError`` so the app's handler maps it to a 400) or
                a query exceeding the complexity caps.
        """
        q = Query()
        if self.query:
            try:
                q = Query.from_rql(self.query)
            except RQLError as e:
                raise ValueError(f"Invalid RQL query: {e}")
        if self.order_by:
            values = [str(v) for v in self.order_by]
            ascending = not values[0].startswith("-")
            q = q.order_by(*(v.lstrip("-") for v in values), ascending=ascending)
        if self.limit is not None or self.offset:
            start = self.offset or 0
            stop = start + self.limit if self.limit is not None else None
            q = q[start:stop]
        return q if q else None
