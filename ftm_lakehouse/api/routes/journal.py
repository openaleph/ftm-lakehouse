"""Journal API routes: bulk write, count, clear.

There is no flush route: a journal is drained by the store that holds it,
and a repository in api mode delegates its whole flush to the server
(``/entities/flush``) rather than streaming rows out to a local writer.
"""

import asyncio

import pyarrow as pa
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

from ftm_lakehouse.api.dependencies import Journal

router = APIRouter()


@router.post("/{dataset}/_api/journal/bulk")
async def journal_bulk(journal: Journal, request: Request) -> PlainTextResponse:
    """Write an Arrow IPC stream of statement rows into the journal.

    Rows are buffered as-is (:meth:`BaseJournalWriter.add_batch`) – the
    sending writer already re-keyed ids and packed every column."""
    body = await request.body()
    if not body:
        return PlainTextResponse("0")

    def _write() -> int:
        batch = pa.ipc.open_stream(pa.py_buffer(body)).read_all()
        with journal.writer() as writer:
            writer.add_batch(batch)
        return int(batch.num_rows)

    try:
        count = await asyncio.to_thread(_write)
    except KeyError as exc:  # a column the statement schema requires
        raise ValueError(f"Missing statement column: {exc}")
    return PlainTextResponse(str(count))


@router.get("/{dataset}/_api/journal/count")
def journal_count(journal: Journal) -> PlainTextResponse:
    """Get the number of rows in the journal."""
    return PlainTextResponse(str(journal.count()))


@router.delete("/{dataset}/_api/journal/clear")
def journal_clear(journal: Journal) -> PlainTextResponse:
    """Delete all rows from the journal without flushing."""
    return PlainTextResponse(str(journal.clear()))
