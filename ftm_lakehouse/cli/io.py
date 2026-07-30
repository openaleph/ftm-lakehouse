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
from followthemoney import EntityProxy
from ftmq.store.lake import LakeStatement

from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

BULK_ORIGIN = "bulk"

Item = TypeVar("Item", EntityProxy, LakeStatement)


def _extract_context_value(i: EntityProxy, key: str) -> str | None:
    """A single string value from an entity's context, else ``None``.

    Aggregated entity JSON serializes context values as lists even for a
    single value (``{"origin": ["crawl"]}``), so a one-element list counts.
    Multiple values are ambiguous at the entity level – return ``None`` so
    per-statement provenance (or the buffer default) applies instead.
    ``StatementEntity`` has no ``context`` slot at all; its statements carry
    origin / fragment themselves.
    """
    value = (getattr(i, "context", None) or {}).get(key)
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)) and len(value) == 1:
        return value[0] if isinstance(value[0], str) else None
    return None


def _extract_origin(i: Item) -> str | None:
    if isinstance(i, EntityProxy):
        return _extract_context_value(i, "origin")
    return i.origin  # Statement


def _extract_fragment(i: Item) -> str | None:
    if isinstance(i, EntityProxy):
        return _extract_context_value(i, "fragment")
    return i.fragment  # Statement


def _bulk_import(
    dataset: Dataset[Any],
    items: Iterable[Item],
    add: Callable[[EntityBuffer, Item], None],
    *,
    origin: str,
    override_origin: bool,
    bulk_size: int,
    last_seen: datetime | None,
    item_name: str,
) -> None:
    repo = dataset.get_entities()
    buffer = EntityBuffer(
        dataset.name, repo.shards, origin, last_seen=last_seen, max_rows=bulk_size
    )
    now = last_seen or datetime.now(timezone.utc)

    for item in logged_items(
        items,
        "Import",
        item_name=item_name,
        logger=dataset._log,
        chunk_size=100_000 if item_name == "Statement" else 10_000,
    ):
        # Per-item provenance: an item's own origin wins unless the caller
        # forces the CLI origin. ``None`` falls through to per-statement
        # origin, then the buffer default.
        item_origin = origin if override_origin else _extract_origin(item)
        fragment = _extract_fragment(item)
        try:
            add(buffer, item, origin=item_origin, fragment=fragment)
        except BufferFullError:
            # Buffer hit its cap before we got to the bulk_size check
            # (e.g. bulk_size > LAKEHOUSE_MAX_BUFFER_ROWS). Drain and
            # retry the failed add so the item isn't dropped.
            repo.write_statements(buffer.flush_buffer(), now=now, batch_size=None)
            add(buffer, item, origin=item_origin, fragment=fragment)
        if len(buffer) >= bulk_size:
            repo.write_statements(buffer.flush_buffer(), now=now, batch_size=None)

    if buffer:
        repo.write_statements(buffer.flush_buffer(), now=now, batch_size=None)


def import_entities(
    dataset: Dataset[Any],
    proxies: Iterable[EntityProxy],
    *,
    bulk_size: int,
    origin: str = BULK_ORIGIN,
    override_origin: bool = False,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import FtM entity proxies straight into the parquet store."""
    _bulk_import(
        dataset,
        proxies,
        EntityBuffer.add_entity,
        origin=origin,
        override_origin=override_origin,
        bulk_size=bulk_size,
        last_seen=last_seen,
        item_name="Entity",
    )


def import_statements(
    dataset: Dataset[Any],
    statements: Iterable[LakeStatement],
    *,
    origin: str = BULK_ORIGIN,
    override_origin: bool = False,
    bulk_size: int,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import FtM ``Statement`` objects straight into the parquet store."""
    _bulk_import(
        dataset,
        statements,
        EntityBuffer.add_statement,
        origin=origin,
        override_origin=override_origin,
        bulk_size=bulk_size,
        last_seen=last_seen,
        item_name="Statement",
    )
