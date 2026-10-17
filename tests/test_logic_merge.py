"""Tests for the pure merge helpers: entity-closed chunking of sorted streams
and run grouping."""

import random
from collections import Counter
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ftm_lakehouse.logic.merge import entity_chunks, run_groups

SCHEMA = pa.schema([("entity_id", pa.string()), ("value", pa.string())])


def _rows(ids: list[str]) -> pa.Table:
    return pa.table(
        {"entity_id": ids, "value": [f"{i}:{e}" for i, e in enumerate(ids)]},
        schema=SCHEMA,
    )


def _stream(table: pa.Table, sizes: list[int]) -> Iterator[pa.RecordBatch]:
    """``table`` in batches of the given sizes (``0`` makes an empty batch),
    cycling through ``sizes``."""
    offset, n = 0, 0
    while offset < table.num_rows:
        size = sizes[n % len(sizes)]
        n += 1
        yield from table.slice(offset, size).to_batches() or [
            pa.RecordBatch.from_pylist([], schema=SCHEMA)
        ]
        offset += size


def _check(chunks: list[pa.Table], source: pa.Table) -> None:
    """Every source row once, no entity in two chunks, chunk ranges ascending."""
    rows = Counter()
    last: str | None = None
    for chunk in chunks:
        rows.update(chunk.column("value").to_pylist())
        ids = chunk.column("entity_id")
        low, high = pc.min(ids).as_py(), pc.max(ids).as_py()
        assert last is None or low > last
        last = high
    assert rows == Counter(source.column("value").to_pylist())


@pytest.mark.parametrize("target", (1, 2_000, 10**9))
def test_entity_chunks_entity_closed(target):
    """Chunks partition the rows by entity whatever the byte target – an entity
    spread over every stream and batch boundary included."""
    random.seed(7)
    universe = [f"e{i:04d}" for i in range(300)]
    tables = []
    for _ in range(6):
        ids = sorted(random.choice(universe) for _ in range(500))
        ids += ["e0150"] * 40  # one entity heavy in every stream
        tables.append(_rows(sorted(ids)))
    tables.append(_rows([]))  # an empty stream
    source = pa.concat_tables(tables)
    streams = [_stream(t, [7, 0, 30, 1]) for t in tables]

    chunks = list(entity_chunks(streams, target))
    _check(chunks, source)
    if target == 1:
        assert len(chunks) > 10
    if target == 10**9:
        assert len(chunks) == 1


def test_entity_chunks_one_stream():
    """A single sorted stream comes out as it went in, cut between entities."""
    table = _rows(sorted(f"e{i % 50:03d}" for i in range(400)))
    chunks = list(entity_chunks([_stream(table, [33])], 1))
    _check(chunks, table)
    assert len(chunks) > 1
    for chunk in chunks:  # rows kept in stream order
        ids = chunk.column("entity_id").to_pylist()
        assert ids == sorted(ids)


def test_entity_chunks_nothing():
    assert list(entity_chunks([], 1)) == []
    assert list(entity_chunks([iter([]), _stream(_rows([]), [5])], 1)) == []


def test_run_groups():
    sizes = [("a", 300), ("b", 300), ("c", 300), ("d", 500), ("e", 1200), ("f", 100)]
    assert run_groups(sizes, 1000) == [["a", "b", "c"], ["d"], ["e"], ["f"]]
    assert run_groups(sizes, 10**6) == [["a", "b", "c", "d", "e", "f"]]
    assert run_groups([("e", 1200)], 1000) == [["e"]]
    assert run_groups([], 1000) == []
