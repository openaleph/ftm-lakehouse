"""Journal API routes: bulk write, iterate, flush, count, clear."""

from typing import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, StreamingResponse

from ftm_lakehouse.api.helpers import Journal
from ftm_lakehouse.storage.journal import JournalRow
from ftm_lakehouse.storage.journal.api import TSV_CONTENT_TYPE, _to_iso, deserialize_row

router = APIRouter()


def _serialize_row(row: JournalRow) -> bytes:
    fields = "\t".join(row[:5])
    return f"{fields}\t{_to_iso(row[5])}\n".encode()


@router.post("/{dataset}/_api/journal/bulk")
async def journal_bulk(journal: Journal, request: Request) -> PlainTextResponse:
    """Write TSV rows into the journal via bulk writer."""
    count = 0
    body = await request.body()
    with journal.writer() as writer:
        for line in body.split(b"\n"):
            if not line:
                continue
            row = deserialize_row(line.decode())
            writer.add(*row)
            count += 1
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/journal/iterate")
async def journal_iterate(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as TSV."""

    async def generate() -> AsyncIterator[bytes]:
        for row in journal.iterate():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=TSV_CONTENT_TYPE)


@router.post("/{dataset}/_api/journal/flush")
async def journal_flush(journal: Journal) -> StreamingResponse:
    """Stream all journal rows as TSV and delete from storage"""

    async def generate() -> AsyncIterator[bytes]:
        for row in journal.flush():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=TSV_CONTENT_TYPE)


@router.get("/{dataset}/_api/journal/count")
async def journal_count(journal: Journal) -> PlainTextResponse:
    """Get the number of rows in the journal."""
    return PlainTextResponse(str(journal.count()))


@router.delete("/{dataset}/_api/journal/clear")
async def journal_clear(journal: Journal) -> PlainTextResponse:
    """Delete all rows from the journal without flushing."""
    return PlainTextResponse(str(journal.clear()))
