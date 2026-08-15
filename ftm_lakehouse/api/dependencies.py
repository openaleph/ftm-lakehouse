"""Shared FastAPI dependencies and constants for API routes."""

from functools import lru_cache
from typing import Annotated

from anystore.types import SDict
from fastapi import Body, Depends, Request
from ftmq.query import Query
from pydantic import BaseModel, ConfigDict, field_validator

from ftm_lakehouse.catalog import get_dataset_model
from ftm_lakehouse.core.settings import ApiSettings
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import LRU_MAX
from ftm_lakehouse.repository.factories import get_entities as _get_entities
from ftm_lakehouse.storage.journal import BaseJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal
from ftm_lakehouse.util import validate_dataset_name

api_settings = ApiSettings()

EMBED = Body(embed=True)
"""Use for single-parameter endpoints so FastAPI expects ``{"<name>": value}``
rather than the bare value as the entire body."""


def get_dataset_name(dataset: str) -> str:
    """Validate the ``{dataset}`` path parameter."""
    return validate_dataset_name(dataset)


DatasetName = Annotated[str, Depends(get_dataset_name)]


def get_dataset_uri(dataset: str, request: Request) -> str:
    """Validated canonical dataset uri under the app's catalog root."""
    return str(request.app.state.lake.dataset_uri(dataset))


DatasetUri = Annotated[str, Depends(get_dataset_uri)]


def get_entities_repo(dataset: str, request: Request) -> EntityRepository:
    """Resolve the entity repository through the LRU-cached factory."""
    return _get_entities(dataset, get_dataset_uri(dataset, request))


Entities = Annotated[EntityRepository, Depends(get_entities_repo)]


@lru_cache(maxsize=LRU_MAX)
def _resolve_shards(dataset: str, uri: str) -> int:
    return get_dataset_model(dataset, uri).shards


def get_dataset_shards(dataset: str, request: Request) -> int:
    """The dataset's recorded shard count.

    Resolved from the dataset's own ``config.yml`` (never the server's
    environment) and cached per ``(dataset, uri)`` – the shard count is
    fixed at dataset creation and must not change, so one read per process
    suffices."""
    return _resolve_shards(dataset, get_dataset_uri(dataset, request))


Shards = Annotated[int, Depends(get_dataset_shards)]


def get_journal(dataset: str) -> BaseJournalStore:
    """Get a JournalStore instance using settings-resolved URI."""
    validate_dataset_name(dataset)
    return _get_journal(dataset)


Journal = Annotated[BaseJournalStore, Depends(get_journal)]


class QueryBody(BaseModel):
    """
    Pydantic model for ``entities`` / ``statements`` query bodies for
    serializing ``ftmq.Query`` through the wire via ``.from_dict()`` /
    ``.to_dict()``.

    :meth:`to_query` enforces the runtime complexity caps from
    :class:`ApiSettings` on the parsed query.

    Use as ``body: QueryBody`` in a route signature; FastAPI parses and
    validates the request body against this model before the handler runs,
    returning a 422 (Pydantic's standard error shape) on any violation.
    """

    model_config = ConfigDict(extra="forbid")

    flush_first: bool = False
    query: SDict | None = None

    @field_validator("query")
    @classmethod
    def validate_query_body(cls, v: SDict | None) -> SDict | None:
        """Enforce the API complexity caps on a parsed query.

        The semantic DoS caps guard here: total filter-leaf count vs
        ``max_filter_keys`` and every ``in`` / ``not_in`` value list vs
        ``max_entity_ids`` – no request can smuggle an unbounded ``IN`` literal
        (which DuckDB chokes on) or filter fan-out past the boundary.

        Raises:
            ValueError: When a cap is exceeded (mapped to a 422 by the app's
                exception handler).
        """
        if not v:
            return v
        try:
            q = Query.from_dict(v)
            if not q:
                # there was payload but not valid query:
                raise ValueError(f"Could not parse `{v}`")
        except Exception as e:
            raise ValueError(f"Invalid query json: `{e}`")
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

    def to_query(self) -> Query:
        """Build the ftmq ``Query`` from the payload.

        Raises:
            ValueError: On malformed payload
        """
        q = Query()
        if self.query:
            try:
                q = Query.from_dict(self.query)
            except Exception as e:
                raise ValueError(f"Invalid query json: `{e}`")
        return q
