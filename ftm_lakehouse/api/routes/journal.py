"""Journal API routes: bulk write, iterate, flush, count, clear."""

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, StreamingResponse

from ftm_lakehouse.api.helpers import Journal
from ftm_lakehouse.storage.journal import JournalRow
from ftm_lakehouse.storage.journal.api import (
    JSONL_CONTENT_TYPE,
    _to_iso,
    deserialize_row,
)

router = APIRouter()


def _serialize_row(row: JournalRow) -> bytes:
    parts = list(row[:5]) + [_to_iso(row[5])]
    return json.dumps(parts, ensure_ascii=False).encode() + b"\n"


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
                row = deserialize_row(line.decode())
                writer.add(*row)
                count += 1
        return count

    count = await asyncio.to_thread(_write)
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/journal/iterate")
def journal_iterate(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as JSONL."""

    def generate():
        for row in journal.iterate():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=JSONL_CONTENT_TYPE)


@router.post("/{dataset}/_api/journal/flush")
def journal_flush(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as JSONL and delete from storage"""

    def generate():
        for row in journal.flush():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=JSONL_CONTENT_TYPE)


@router.get("/{dataset}/_api/journal/count")
def journal_count(journal: Journal) -> PlainTextResponse:
    """Get the number of rows in the journal."""
    return PlainTextResponse(str(journal.count()))


@router.delete("/{dataset}/_api/journal/clear")
def journal_clear(journal: Journal) -> PlainTextResponse:
    """Delete all rows from the journal without flushing."""
    return PlainTextResponse(str(journal.clear()))
