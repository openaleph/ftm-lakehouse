"""Tests for the EntityBuffer memory bounds.

Guards the trust boundary: a single malicious tenant must not be able to
OOM the writer by filling the in-memory buffer past its cap. ``max_rows``
is the only bound on the write path – including when every entity-id is
made to collide onto one shard, so the whole buffer drains as one table.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from followthemoney import Statement, model

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import SHARDED_SCHEMA
from ftm_lakehouse.repository.entities.main import EntityRepository


def _stmt(i: int) -> Statement:
    return Statement(
        id=f"stmt-{i}",
        entity_id=f"entity-{i}",
        prop="name",
        schema="Person",
        value=f"Name {i}",
        dataset="test",
        origin="default",
    )


# --- EntityBuffer cap -------------------------------------------------------


def test_entity_buffer_respects_max_rows() -> None:
    buf = EntityBuffer("test", shards=8, max_rows=5)
    for i in range(5):
        buf.add_statement(_stmt(i))
    assert len(buf) == 5

    with pytest.raises(BufferFullError):
        buf.add_statement(_stmt(99))


def test_entity_buffer_flush_releases_capacity() -> None:
    buf = EntityBuffer("test", shards=8, max_rows=3)
    for i in range(3):
        buf.add_statement(_stmt(i))
    with pytest.raises(BufferFullError):
        buf.add_statement(_stmt(4))

    drained = list(buf.flush_buffer())
    assert len(drained) == 3
    assert len(buf) == 0

    # capacity is restored
    buf.add_statement(_stmt(4))
    assert len(buf) == 1


def test_entity_buffer_add_entity_rejects_when_full() -> None:
    buf = EntityBuffer("test", shards=8, max_rows=2)
    buf.add_statement(_stmt(0))
    buf.add_statement(_stmt(1))

    entity = model.make_entity("Person")
    entity.id = "would-overflow"
    entity.add("name", "Doesnt Matter")

    with pytest.raises(BufferFullError):
        buf.add_entity(entity)
    # Partial entity must NOT have been buffered.
    assert len(buf) == 2


def test_entity_buffer_default_max_rows_from_settings() -> None:
    buf = EntityBuffer("test", shards=8)
    assert buf.max_rows == Settings().max_buffer_rows


# --- flush_tables: the drain the parquet write path consumes ---------------


def _stub_append(repo, captured: list[pa.Table]) -> None:
    """Replace the underlying parquet append with an in-memory capture."""

    def _append(batch: pa.Table) -> None:
        captured.append(batch)

    repo._statements.append = _append  # type: ignore[method-assign]


def test_flush_tables_emits_one_table_per_shard() -> None:
    """Each drained table is shard-scoped – one parquet file per partition."""
    buf = EntityBuffer("test", shards=8)
    for i in range(200):
        buf.add_statement(_stmt(i))

    tables = list(buf.flush_tables(datetime.now(timezone.utc)))

    assert len(tables) > 1, "200 ids should spread over more than one shard"
    assert all(len(pc.unique(t["shard"])) == 1 for t in tables)
    assert all(t.schema.equals(SHARDED_SCHEMA) for t in tables)
    assert sum(len(t) for t in tables) == 200
    assert len(buf) == 0


def test_flush_tables_abandoned_keeps_the_rest() -> None:
    """A consumer that stops mid-drain keeps what it never saw – and only that.

    Rows leave the buffer as they are handed over, so an abandoned drain
    neither loses a shard nor re-emits one that already went downstream.
    """
    buf = EntityBuffer("test", shards=8)
    for i in range(200):
        buf.add_statement(_stmt(i))

    tables = buf.flush_tables(datetime.now(timezone.utc))
    first = next(tables)
    tables.close()

    remaining = 200 - len(first)
    assert len(buf) == remaining
    assert sum(len(t) for t in buf.flush_tables()) == remaining


def test_flush_tables_single_shard_collision_bounded_by_cap(tmp_path) -> None:
    """Colliding every id onto one shard stays bounded by the buffer cap."""
    repo = EntityRepository("test", tmp_path)
    captured: list[pa.Table] = []
    _stub_append(repo, captured)

    # shards <= 1 is the single-shard sentinel: every row lands in shard "0"
    buf = EntityBuffer("test", shards=0, max_rows=50)
    for i in range(50):
        buf.add_statement(_stmt(i))
    with pytest.raises(BufferFullError):
        buf.add_statement(_stmt(99))

    total = repo.write_batches(buf.flush_tables(datetime.now(timezone.utc)))

    assert total == 50
    # one shard, one table – capped by max_rows, never by the row count
    assert len(captured) == 1
    assert len(captured[0]) == 50
