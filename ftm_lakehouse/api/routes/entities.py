"""Entity API routes: flush, query, delete, stats, version."""

from typing import Annotated, Optional

import orjson
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.api.dependencies import EMBED, Dataset, QueryBody

NDJSON_CONTENT_TYPE = "application/x-ndjson"

router = APIRouter()


@router.post("/{dataset}/_api/entities/flush")
def entities_flush(dataset: Dataset) -> PlainTextResponse:
    """Flush journal to parquet store, return count of new statements."""
    count = dataset.get_entities().flush()
    return PlainTextResponse(str(count))


@router.post("/{dataset}/_api/entities/merge")
def entities_merge(
    dataset: Dataset,
    grace_period_days: Annotated[Optional[int], EMBED] = None,
) -> PlainTextResponse:
    """Collapse duplicates and reap expired tombstones from parquet store"""
    dataset.get_entities().merge(grace_period_days)
    return PlainTextResponse("ok")


@router.post("/{dataset}/_api/entities/query")
def entities_query(dataset: Dataset, body: QueryBody) -> StreamingResponse:
    """Query entities from parquet store, streamed as NDJSON."""
    # Parse (and thereby validate) the query BEFORE streaming starts – an
    # invalid body must 400, not break the stream after 200 + headers.
    query = body.to_query()

    def generate():
        for entity in dataset.get_entities().query(
            query,
            flush_first=body.flush_first,
            origin=body.origin,
        ):
            yield orjson.dumps(entity.to_dict(), option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)


@router.delete("/{dataset}/_api/entities/{entity_id}")
def entities_delete(dataset: Dataset, entity_id: str) -> PlainTextResponse:
    """Delete all statements for an entity, return count of tombstones."""
    count = dataset.get_entities().delete_entity(entity_id)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/entities/stats")
def entities_stats(dataset: Dataset) -> DatasetStats:
    """Return dataset statistics from parquet store."""
    return dataset.get_entities().get_statistics()


@router.get("/{dataset}/_api/entities/statements/version")
def entities_version(dataset: Dataset) -> PlainTextResponse:
    """Return current Delta table version."""
    v = dataset.get_entities()._statements.version
    return PlainTextResponse(str(v or 0))


@router.post("/{dataset}/_api/entities/statements/query")
def statements_query(dataset: Dataset, body: QueryBody) -> StreamingResponse:
    """Query statements from parquet store, streamed as NDJSON.

    Honors the full body contract: ``origin`` applies as a storage-level row
    filter, ``flush_first`` drains the journal before reading. Each line carries
    the statement's ``fragment`` alongside the followthemoney fields so the
    supersession group key survives the wire (``Statement.to_dict`` has no
    notion of it).
    """
    # Parse (and thereby validate) the query BEFORE streaming starts.
    query = body.to_query()
    repo = dataset.get_entities()
    if body.flush_first:
        repo.flush()

    def generate():
        for statement in repo.query_statements(query, origin=body.origin):
            data = {**statement.to_dict(), "fragment": statement.fragment}
            yield orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)
