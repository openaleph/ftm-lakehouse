"""Tests for the EntityBuffer memory bounds.

Guards the trust boundary: a single malicious tenant must not be able to
OOM the writer by filling the in-memory buffer past its cap. ``max_rows``
is the only bound on the write path: the buffer holds no partition key, so
however the entity ids are distributed the whole thing drains as one table.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from followthemoney import Statement, model

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import JOURNAL_SCHEMA
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
    buf = EntityBuffer("test", max_rows=5)
    for i in range(5):
        buf.add_statement(_stmt(i))
    assert len(buf) == 5

    with pytest.raises(BufferFullError):
        buf.add_statement(_stmt(99))


def test_entity_buffer_flush_releases_capacity() -> None:
    buf = EntityBuffer("test", max_rows=3)
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
    buf = EntityBuffer("test", max_rows=2)
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
    buf = EntityBuffer("test")
    assert buf.max_rows == Settings().max_buffer_rows


# --- flush_table: the drain the parquet write path consumes ----------------


def _stub_append(repo, captured: list[pa.Table]) -> None:
    """Replace the underlying parquet append with an in-memory capture."""

    def _append(batch: pa.Table) -> None:
        captured.append(batch)

    repo._statements.append = _append  # type: ignore[method-assign]


def test_flush_table_packs_the_whole_buffer() -> None:
    """The drain is one table, carrying no partition key.

    ``shard`` is derived by ``ParquetStore.append``, so the buffer neither
    groups by it nor packs it – however the entity ids are distributed, the
    whole buffer goes over in a single ``JOURNAL_SCHEMA`` table.
    """
    buf = EntityBuffer("test")
    for i in range(200):
        buf.add_statement(_stmt(i))

    table = buf.flush_table(datetime.now(timezone.utc))

    assert table.schema.equals(JOURNAL_SCHEMA)
    assert "shard" not in table.schema.names
    assert len(table) == 200
    assert len(buf) == 0
    # drained, not copied: a second flush has nothing left to hand over
    assert len(buf.flush_table()) == 0


def test_flush_table_bounded_by_cap(tmp_path) -> None:
    """The cap – not the input size – is what bounds a drain."""
    repo = EntityRepository("test", tmp_path)
    captured: list[pa.Table] = []
    _stub_append(repo, captured)

    buf = EntityBuffer("test", max_rows=50)
    for i in range(50):
        buf.add_statement(_stmt(i))
    with pytest.raises(BufferFullError):
        buf.add_statement(_stmt(99))

    total = repo.write_batches([buf.flush_table(datetime.now(timezone.utc))])

    assert total == 50
    assert len(captured) == 1
    assert len(captured[0]) == 50
