"""Arrow IPC framing for the lakehouse http api.

The stores speak Arrow, so the wire does too: endpoints that move statement
rows carry an Arrow IPC *stream* – the same batches both sides already hold,
with no row format in between. Framing lives here, next to the byte
transport in :mod:`ftm_lakehouse.core.api`, so a second Arrow endpoint does
not have to reach into a storage module for it.
"""

import io
from typing import Iterable, Iterator

import pyarrow as pa

ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

COMPRESSION = "zstd"
"""Body compression for the wire – statement rows shrink ~4-5x.

Buffer-level IPC compression, so the framing stays a plain Arrow stream:
``pa.ipc.open_stream`` decompresses transparently and no reader has to know.
Statement columns are highly repetitive (dataset, origin, schema, prop, the
timestamps), which is what a shipping crawler pays for on its uplink –
compressing costs a fraction of the time the bytes would spend on the wire.
"""

WRITE_OPTIONS = pa.ipc.IpcWriteOptions(compression=COMPRESSION)
"""Shared writer options – zstd compresses stateless, so one is enough."""


def serialize_batches(
    batches: Iterable[pa.Table | pa.RecordBatch], schema: pa.Schema
) -> Iterator[bytes]:
    """Serialize tables or record batches as one Arrow IPC stream.

    The buffer is drained and truncated after every batch so a response
    streams instead of materializing: safe because the IPC *stream* format is
    a forward-only sequence of messages with no back-references (unlike the
    file format, whose footer indexes absolute offsets).

    Record batch buffers are :data:`COMPRESSION`-compressed on the way out.

    Args:
        batches: The tables or batches to write.
        schema: Their schema – written as the stream header.

    Yields:
        The stream's bytes: one chunk per item written (a chunked table
        writes one message per chunk), then the end-of-stream marker (which
        carries the header when there was nothing at all).
    """
    buffer = io.BytesIO()
    with pa.ipc.new_stream(buffer, schema, options=WRITE_OPTIONS) as writer:
        for batch in batches:
            writer.write(batch)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)
    yield buffer.getvalue()


def serialize_table(table: pa.Table) -> bytes:
    """Serialize a table as one self-contained Arrow IPC stream."""
    return b"".join(serialize_batches([table], table.schema))
