"""Journal API routes: bulk write, iterate, flush, count, clear."""

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, StreamingResponse

from ftm_lakehouse.api.dependencies import Journal
from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.storage.journal.api import (
    JSONL_CONTENT_TYPE,
    deserialize_row,
    serialize_row,
)

router = APIRouter()


@router.post("/{dataset}/_api/journal/bulk")
async def journal_bulk(journal: Journal, request: Request) -> PlainTextResponse:
    """Write JSONL rows into the journal via bulk writer."""
    body = await request.body()

    def _write() -> int:
        count = 0
        with journal.writer() as writer:
            for line in body.split(b"\n"):
                if not line:
                    continue
                # FIXME this is a bit inefficient as the writer will re-pack the
                # statement again for the journal.
                row = deserialize_row(line.decode())
                stmt = unpack_statement(row.data)
                writer.add_statement(stmt, row.deleted_at)
                count += 1
        return count

    count = await asyncio.to_thread(_write)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/journal/iterate")
def journal_iterate(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as JSONL."""

    def generate():
        for row in journal.iterate():
            yield serialize_row(row) + b"\n"

    return StreamingResponse(generate(), media_type=JSONL_CONTENT_TYPE)


@router.post("/{dataset}/_api/journal/flush")
def journal_flush(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as JSONL and delete from storage"""

    def generate():
        for row in journal.flush():
            yield serialize_row(row) + b"\n"

    return StreamingResponse(generate(), media_type=JSONL_CONTENT_TYPE)


@router.get("/{dataset}/_api/journal/count")
def journal_count(journal: Journal) -> PlainTextResponse:
    """Get the number of rows in the journal."""
    return PlainTextResponse(str(journal.count()))


@router.delete("/{dataset}/_api/journal/clear")
def journal_clear(journal: Journal) -> PlainTextResponse:
    """Delete all rows from the journal without flushing."""
    return PlainTextResponse(str(journal.clear()))
