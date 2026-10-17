"""Sorted-run helpers for
[`merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge] – pure Arrow.

A merge reads a partition as ``entity_id``-sorted streams (``merged-*`` files
in file order, ``part-*`` files sorted into runs) and cuts them into
entity-closed chunks, each small enough for the dedupe query to run in memory,
so the partition itself is never sorted or held whole.
"""

import heapq
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc


def run_groups(sizes: Iterable[tuple[str, int]], budget: int) -> list[list[str]]:
    """Group ``(file, bytes)`` pairs in order, each group filled up to ``budget``.

    A file larger than the budget forms a group of its own.
    """
    groups: list[list[str]] = []
    group: list[str] = []
    total = 0
    for file, size in sizes:
        if group and total + size > budget:
            groups.append(group)
            group, total = [], 0
        group.append(file)
        total += size
    if group:
        groups.append(group)
    return groups


def entity_chunks(
    streams: Iterable[Iterator[pa.RecordBatch]], target: int
) -> Iterator[pa.Table]:
    """Cut ``entity_id``-sorted streams into entity-closed chunks of about
    ``target`` Arrow bytes, in ascending ``entity_id`` ranges.

    A chunk holds every row of each of its entities – across all streams – and
    no two chunks share an entity, so a query keyed on ``entity_id`` gives the
    same rows per chunk as over the whole. Rows within a chunk are not sorted.
    Memory is the chunk plus about one batch per stream.

    Each step advances the stream whose newest batch ends lowest; every row
    below the lowest such end is complete, as no stream has more of it to give.

    Args:
        streams: Record batches sharing one schema, each stream ascending by
            ``entity_id``.
        target: Arrow bytes to read between two chunks.

    Yields:
        ``pa.Table`` per chunk.
    """
    sources = [iter(stream) for stream in streams]
    ends: list[tuple[str, int]] = []  # (last entity_id read, stream) heap
    held: list[pa.RecordBatch] = []
    since = 0

    def pull(i: int) -> None:
        nonlocal since
        for batch in sources[i]:
            if batch.num_rows:
                held.append(batch)
                since += batch.nbytes
                heapq.heappush(ends, (batch.column("entity_id")[-1].as_py(), i))
                return

    for i in range(len(sources)):
        pull(i)
    while ends:
        _, i = heapq.heappop(ends)
        pull(i)
        if (ends and since < target) or not held:
            continue
        table = pa.Table.from_batches(held)
        since = 0
        if not ends:
            held = []
            yield table
            continue
        below = pc.less(table.column("entity_id"), ends[0][0])
        held = table.filter(pc.invert(below)).to_batches()
        chunk = table.filter(below)
        if chunk.num_rows:
            yield chunk
