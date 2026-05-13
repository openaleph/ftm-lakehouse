"""Entity API routes: flush, query, delete, stats, version."""

from typing import Annotated, Optional

import orjson
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
from ftmq.model.stats import DatasetStats
from ftmq.query import Query

from ftm_lakehouse.api.dependencies import EMBED, Dataset, QueryBody

NDJSON_CONTENT_TYPE = "application/x-ndjson"

router = APIRouter()


@router.post("/{dataset}/_api/entities/flush")
def entities_flush(dataset: Dataset) -> PlainTextResponse:
    """Flush journal to parquet store, return count of new statements."""
    count = dataset.entities.flush()
    return PlainTextResponse(str(count))


@router.post("/{dataset}/_api/entities/merge")
def entities_merge(
    dataset: Dataset,
    grace_period_days: Annotated[Optional[int], EMBED] = None,
) -> PlainTextResponse:
    """Collapse duplicates and reap expired tombstones from parquet store"""
    dataset.entities.merge(grace_period_days)
    return PlainTextResponse("ok")


@router.post("/{dataset}/_api/entities/query")
def entities_query(dataset: Dataset, body: QueryBody) -> StreamingResponse:
    """Query entities from parquet store, streamed as NDJSON."""

    def generate():
        for entity in dataset.entities.query(
            entity_ids=body.entity_ids,
            flush_first=body.flush_first,
            **body.filter_kwargs(),
        ):
            yield orjson.dumps(entity.to_dict(), option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)


@router.delete("/{dataset}/_api/entities/{entity_id}")
def entities_delete(dataset: Dataset, entity_id: str) -> PlainTextResponse:
    """Delete all statements for an entity, return count of tombstones."""
    count = dataset.entities.delete_entity(entity_id)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/entities/stats")
def entities_stats(dataset: Dataset) -> DatasetStats:
    """Return dataset statistics from parquet store."""
    return dataset.entities.get_statistics()


@router.get("/{dataset}/_api/entities/statements/version")
def entities_version(dataset: Dataset) -> PlainTextResponse:
    """Return current Delta table version."""
    v = dataset.entities._statements.version
    return PlainTextResponse(str(v or 0))


@router.post("/{dataset}/_api/entities/statements/query")
def statements_query(dataset: Dataset, body: QueryBody) -> StreamingResponse:
    """Query statements from parquet store, streamed as NDJSON."""
    query = Query().where(**body.filter_kwargs())
    sql = query.sql.statements

    def generate():
        for statement in dataset.entities._statements.query_statements(sql):
            yield orjson.dumps(statement.to_dict(), option=orjson.OPT_APPEND_NEWLINE)

    return StreamingResponse(generate(), media_type=NDJSON_CONTENT_TYPE)
