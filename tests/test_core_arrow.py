"""Arrow IPC framing for the http api."""

import io
from datetime import datetime, timezone

import pyarrow as pa

from ftm_lakehouse.core.arrow import serialize_batches, serialize_table
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    LakehouseStatement,
    statements_to_arrow,
)

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def make_table(rows: int = 3000) -> pa.Table:
    return statements_to_arrow(
        [
            LakehouseStatement(
                entity_id=f"e{i}",
                prop="name",
                schema="Person",
                value=f"value {i}" * 20,
                dataset="test",
            )
            for i in range(rows)
        ],
        NOW,
    )


def read(payload: bytes) -> pa.Table:
    """Decode a payload the way the bulk route does."""
    return pa.ipc.open_stream(pa.py_buffer(payload)).read_all()


def test_core_arrow_table_round_trip():
    table = make_table(rows=10)
    assert read(serialize_table(table)).equals(table)


def test_core_arrow_round_trip_keeps_the_schema():
    """Nullability included – the wire carries the statement contract."""
    decoded = read(serialize_table(make_table(rows=10)))
    assert decoded.schema.equals(SHARDED_SCHEMA)


def test_core_arrow_writes_a_chunk_per_message():
    """A chunked table goes out chunk by chunk, so a stream can be written
    without materializing it."""
    table = make_table()
    chunked = pa.Table.from_batches(
        table.to_batches(max_chunksize=500), schema=SHARDED_SCHEMA
    )
    chunks = list(serialize_batches([chunked], SHARDED_SCHEMA))
    assert len(chunks) > 1  # one per chunk, plus the end-of-stream marker
    assert read(b"".join(chunks)).equals(table)


def test_core_arrow_stream_is_compressed():
    """Buffers go out compressed – the uplink is the write path's bottleneck,
    not the packing."""
    table = make_table()
    payload = serialize_table(table)
    plain = io.BytesIO()
    with pa.ipc.new_stream(plain, table.schema) as writer:
        writer.write(table)
    assert len(payload) * 2 < len(plain.getvalue())
    assert read(payload).equals(table)


def test_core_arrow_empty_stream_carries_the_schema():
    """Nothing to write still frames a valid stream."""
    decoded = read(b"".join(serialize_batches([], SHARDED_SCHEMA)))
    assert decoded.schema.equals(SHARDED_SCHEMA)
    assert decoded.num_rows == 0
