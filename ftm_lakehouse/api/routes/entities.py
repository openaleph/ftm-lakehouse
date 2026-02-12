"""Entity API routes: flush, query, delete, stats, version."""

from typing import AsyncIterator

import orjson
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from ftmq.model.stats import DatasetStats
from ftmq.query import Query

from ftm_lakehouse.api.helpers import NDJSON_CONTENT_TYPE, Dataset

router = APIRouter()


@router.post("/{dataset}/_api/entities/flush")
async def entities_flush(dataset: Dataset) -> PlainTextResponse:
    """Flush journal to parquet store, return count of new statements."""
    count = dataset.entities.flush()
    return PlainTextResponse(str(count))


@router.post("/{dataset}/_api/entities/query")
async def entities_query(dataset: Dataset, request: Request) -> StreamingResponse:
    """Query entities from parquet store, streamed as NDJSON."""
    body = await request.json()
    entity_ids = body.pop("entity_ids", None) or None
    flush_first = body.pop("flush_first", False)

    async def generate() -> AsyncIterator[bytes]:
        for entity in dataset.entities.query(
            entity_ids=entity_ids,
            flush_first=flush_first,
            **body,
        ):
            yield orjson.dumps(entity.to_dict(), option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)


@router.delete("/{dataset}/_api/entities/{entity_id}")
async def entities_delete(dataset: Dataset, entity_id: str) -> PlainTextResponse:
    """Delete all statements for an entity, return count of tombstones."""
    count = dataset.entities.delete_entity(entity_id)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/entities/stats")
async def entities_stats(dataset: Dataset) -> DatasetStats:
    """Return dataset statistics from parquet store."""
    return dataset.entities.get_statistics()


@router.get("/{dataset}/_api/entities/statements/version")
async def entities_version(dataset: Dataset) -> PlainTextResponse:
    """Return current Delta table version."""
    v = dataset.entities._statements.version
    return PlainTextResponse(str(v or 0))


@router.post("/{dataset}/_api/entities/statements/query")
async def statements_query(dataset: Dataset, request: Request) -> StreamingResponse:
    """Query estatements from parquet store, streamed as NDJSON."""
    body = await request.json()
    query = Query().where(**body)
    sql = query.sql.statements

    async def generate() -> AsyncIterator[bytes]:
        for statement in dataset.entities._statements.query_statements(sql):
            yield orjson.dumps(statement.to_dict(), option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)
