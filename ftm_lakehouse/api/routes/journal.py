"""Journal API routes: bulk write, iterate, flush, count, clear."""

from functools import cache
from typing import Annotated, AsyncIterator

from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import PlainTextResponse, StreamingResponse

from ftm_lakehouse.storage.journal import JournalRow, JournalStore
from ftm_lakehouse.storage.journal.api import TSV_CONTENT_TYPE, deserialize_row, _to_iso

router = APIRouter()


def _serialize_row(row: JournalRow) -> bytes:
    return f"{'\t'.join(row[:5])}\t{_to_iso(row[5])}\n".encode()


@cache
def _get_journal(dataset: str, uri: str) -> JournalStore:
    return JournalStore(dataset, uri)


def get_journal(dataset: str, request: Request) -> JournalStore:
    """Get a JournalStore instance using the configured URI."""
    journal_uri = request.app.state.journal_uri
    return _get_journal(dataset, journal_uri)


Journal = Annotated[JournalStore, Depends(get_journal)]


@router.post("/{dataset}/journal/bulk")
async def journal_bulk(journal: Journal, request: Request) -> dict:
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
    return {"status": "ok", "count": count}


@router.get("/{dataset}/journal/iterate")
async def journal_iterate(journal: Journal) -> Response:
    """Stream all journal rows as TSV."""

    async def generate() -> AsyncIterator[bytes]:
        for row in journal.iterate():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=TSV_CONTENT_TYPE)


@router.post("/{dataset}/journal/flush")
async def journal_flush(journal: Journal) -> Response:
    """Stream all journal rows as TSV and delete from storage"""

    async def generate() -> AsyncIterator[bytes]:
        for row in journal.flush():
            yield _serialize_row(row)

    return StreamingResponse(generate(), media_type=TSV_CONTENT_TYPE)


@router.get("/{dataset}/journal/count")
async def journal_count(journal: Journal) -> PlainTextResponse:
    """Get the number of rows in the journal."""
    return PlainTextResponse(str(journal.count()))


@router.delete("/{dataset}/journal/clear")
async def journal_clear(journal: Journal) -> PlainTextResponse:
    """Delete all rows from the journal without flushing."""
    return PlainTextResponse(str(journal.clear()))
