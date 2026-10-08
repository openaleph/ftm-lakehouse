"""Entity API routes: flush, query, delete, stats, version."""

import orjson
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.api.dependencies import Entities, QueryBody

NDJSON_CONTENT_TYPE = "application/x-ndjson"

router = APIRouter()


@router.post("/{dataset}/_api/entities/flush")
def entities_flush(entities: Entities) -> PlainTextResponse:
    """Flush journal to parquet store, return count of new statements."""
    count = entities.flush()
    return PlainTextResponse(str(count))


@router.post("/{dataset}/_api/entities/query")
def entities_query(entities: Entities, body: QueryBody) -> StreamingResponse:
    """Query entities from parquet store, streamed as NDJSON."""
    # parse and flush before the stream starts: a bad body or a failing flush
    # must fail the request, not cut a 200 response short
    query = body.to_query()
    if body.flush_first:
        entities.flush()

    def generate():
        for entity in entities.query(query):
            yield orjson.dumps(
                entity.to_statement_dict(), option=orjson.OPT_APPEND_NEWLINE
            )

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)


@router.delete("/{dataset}/_api/entities/origins/{origin}")
def entities_delete_origin(entities: Entities, origin: str) -> PlainTextResponse:
    """Physically drop a whole origin partition from the parquet store."""
    entities.delete_origin(origin)
    return PlainTextResponse("ok")


@router.delete("/{dataset}/_api/entities/{entity_id}")
def entities_delete(
    entities: Entities, entity_id: str, origin: str | None = None
) -> PlainTextResponse:
    """Delete all statements for an entity, return count of tombstones.

    ``origin`` narrows the delete to that origin's statements only.
    """
    count = entities.delete_entity(entity_id, origin)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/entities/stats")
def entities_stats(entities: Entities) -> DatasetStats:
    """Return dataset statistics from parquet store."""
    return entities.stats()


@router.get("/{dataset}/_api/entities/statements/version")
def entities_version(entities: Entities) -> PlainTextResponse:
    """Return current Delta table version."""
    v = entities.version
    # empty for no table – the client reads that back as `None`, as locally
    return PlainTextResponse("" if v is None else str(v))


@router.post("/{dataset}/_api/entities/statements/query")
def statements_query(entities: Entities, body: QueryBody) -> StreamingResponse:
    """Query statements from parquet store, streamed as NDJSON."""
    # before the stream starts, as in `entities_query`
    query = body.to_query()
    if body.flush_first:
        entities.flush()

    def generate():
        for row in entities.query_statements_data(query):
            yield orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)
