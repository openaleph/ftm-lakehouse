"""Shared bulk-import loop for the CLI ``import`` commands.

Both ``entities import`` and ``statements import`` stream items through an
in-memory :class:`EntityBuffer` (pre-sorted by shard) and hand full batches
to ``EntityRepository.write_statements`` for a per-shard parquet append,
bypassing the journal. The loop here is the single implementation; the
command modules only differ in how they parse their input.
"""

from datetime import datetime, timezone
from typing import Any, Callable, Iterable, TypeVar

from anystore.io import logged_items
from followthemoney import EntityProxy, Statement

from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

BULK_ORIGIN = "bulk"

Item = TypeVar("Item", EntityProxy, Statement)


def _bulk_import(
    dataset: Dataset[Any],
    items: Iterable[Item],
    add: Callable[[EntityBuffer, Item], None],
    *,
    origin: str,
    bulk_size: int,
    last_seen: datetime | None,
    item_name: str,
) -> None:
    repo = dataset.get_entities()
    buffer = EntityBuffer(dataset.name, repo.shards, origin)
    now = last_seen or datetime.now(timezone.utc)

    for item in logged_items(items, "Write", item_name=item_name, logger=dataset._log):
        try:
            add(buffer, item)
        except BufferFullError:
            # Buffer hit its cap before we got to the bulk_size check
            # (e.g. bulk_size > LAKEHOUSE_MAX_BUFFER_ROWS). Drain and
            # retry the failed add so the item isn't dropped.
            repo.write_statements(buffer.flush_buffer(), now=now)
            add(buffer, item)
        if len(buffer) >= bulk_size:
            repo.write_statements(buffer.flush_buffer(), now=now)

    if buffer:
        repo.write_statements(buffer.flush_buffer(), now=now)


def import_entities(
    dataset: Dataset[Any],
    proxies: Iterable[EntityProxy],
    *,
    origin: str,
    bulk_size: int,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import FtM entity proxies straight into the parquet store."""
    _bulk_import(
        dataset,
        proxies,
        EntityBuffer.add_entity,
        origin=origin,
        bulk_size=bulk_size,
        last_seen=last_seen,
        item_name="Entity",
    )


def import_statements(
    dataset: Dataset[Any],
    statements: Iterable[Statement],
    *,
    origin: str,
    bulk_size: int,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import FtM ``Statement`` objects straight into the parquet store."""
    _bulk_import(
        dataset,
        statements,
        EntityBuffer.add_statement,
        origin=origin,
        bulk_size=bulk_size,
        last_seen=last_seen,
        item_name="Statement",
    )
