"""Tests for the EntityBuffer / write_statements memory bounds.

Guards the trust boundary: a single malicious tenant must not be able to
OOM the writer by filling the in-memory buffer past its cap, nor by
colliding entity-ids onto one shard so the per-shard write buffer grows
unboundedly.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from followthemoney import Statement, model

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import SHARDED_SCHEMA, StatementRow
from ftm_lakehouse.repository.entities.main import (
    WRITE_SHARD_BATCH,
    EntityRepository,
)


def _stmt(i: int) -> Statement:
    return Statement(
        id=f"stmt-{i}",
        entity_id=f"entity-{i}",
        canonical_id=f"entity-{i}",
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


# --- write_statements per-shard interim flush ------------------------------


def _stub_append(repo, captured: list[pa.Table]) -> None:
    """Replace the underlying parquet append with an in-memory capture."""

    def _append(batch: pa.Table) -> None:
        captured.append(batch)

    repo._statements.append = _append  # type: ignore[method-assign]


def test_write_statements_emits_interim_within_shard(tmp_path) -> None:
    """A single huge shard must not be buffered in one giant pa.Table."""
    repo = EntityRepository("test", tmp_path)
    captured: list[pa.Table] = []
    _stub_append(repo, captured)

    # Synthetic shard-sorted stream where every row hashes to shard "0"
    n = WRITE_SHARD_BATCH * 2 + 1

    def stream():
        for i in range(n):
            yield StatementRow("0", _stmt(i), None)

    total = repo.write_statements(stream(), now=datetime.now(timezone.utc))

    assert total == n
    # Three batches: cap, cap, remainder.
    assert len(captured) == 3
    # No interim batch exceeds the cap.
    assert all(len(b) <= WRITE_SHARD_BATCH for b in captured)
    # Schema is preserved and total matches.
    assert all(b.schema.equals(SHARDED_SCHEMA) for b in captured)
    assert sum(len(b) for b in captured) == n


def test_write_statements_single_shard_below_batch_emits_once(tmp_path) -> None:
    """Below the cap, write_statements emits one batch."""
    repo = EntityRepository("test", tmp_path)
    captured: list[pa.Table] = []
    _stub_append(repo, captured)

    n = WRITE_SHARD_BATCH // 10

    def stream():
        for i in range(n):
            yield StatementRow("0", _stmt(i), None)

    total = repo.write_statements(stream(), now=datetime.now(timezone.utc))
    assert total == n
    assert len(captured) == 1
