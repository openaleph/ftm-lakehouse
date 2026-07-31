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
from anystore.types import SDict
from followthemoney import EntityProxy
from ftmq.store.lake import LakeStatement
from rigour.time import iso_datetime

from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.logic.entities.explode import (
    RowBuffer,
    explode_unsafe,
    statement_row_unsafe,
)
from ftm_lakehouse.util import single_string, validate_origin

BULK_ORIGIN = "bulk"

Item = TypeVar("Item", EntityProxy, LakeStatement)


def _extract_context_value(i: EntityProxy, key: str) -> str | None:
    """A single string value from an entity's context, else ``None``.

    Proxy-context twin of the dict extraction in the unsafe explode path –
    both defer to :func:`ftm_lakehouse.util.single_string` (one-element
    lists count, multiple values are ambiguous) so per-statement provenance
    or the buffer default applies on fallback. ``StatementEntity`` has no
    ``context`` slot at all; its statements carry origin / fragment
    themselves.
    """
    return single_string((getattr(i, "context", None) or {}).get(key))


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
    add: Callable[[EntityBuffer, Item], str | None],
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


def import_entities_unsafe(
    dataset: Dataset[Any],
    payloads: Iterable[SDict],
    *,
    origin: str = BULK_ORIGIN,
    override_origin: bool = False,
    bulk_size: int,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import aggregated FtM entity dicts without FtM object construction.

    The ``--unsafe`` fast path: payloads go through :func:`explode_unsafe`
    straight to packed parquet rows – same statement ids, namespace
    stripping and timestamp pinning as :func:`import_entities`, minus
    validation and the per-statement object churn. Trusted input only.
    """
    validate_origin(origin)
    repo = dataset.get_entities()
    # Parity with _bulk_import: --last-seen doubles as the stamp for rows
    # missing their timestamps, not just the pinned last_seen default.
    now = last_seen or datetime.now(timezone.utc)
    # The pinned value the safe path would write for --last-seen: its
    # isoformat round-tripped through the second-granularity ISO parse.
    pinned = iso_datetime(last_seen.isoformat()) if last_seen else None
    buffer = RowBuffer()
    for data in logged_items(
        payloads, "Import", item_name="Entity", logger=dataset._log, chunk_size=10_000
    ):
        for row in explode_unsafe(
            data,
            dataset.name,
            repo.shards,
            now=now,
            origin=origin,
            override_origin=override_origin,
            last_seen=pinned,
        ):
            buffer.add(row)
            # Checked per row (not per payload) so one pathologically large
            # entity cannot grow the buffer past the bulk_size memory bound.
            if len(buffer) >= bulk_size:
                repo.write_rows(buffer.flush(), batch_size=None)
    if buffer:
        repo.write_rows(buffer.flush(), batch_size=None)


def import_statements_unsafe(
    dataset: Dataset[Any],
    rows: Iterable[SDict],
    *,
    origin: str = BULK_ORIGIN,
    bulk_size: int,
    last_seen: datetime | None = None,
) -> None:
    """Bulk-import ``statements.csv`` row dicts without Statement construction.

    The ``--unsafe`` fast path: CSV rows go through
    :func:`statement_row_unsafe` straight to packed parquet rows, mirroring
    :func:`import_statements` field by field – including ``last_seen``
    doubling as the stamp for rows missing their timestamps. Trusted input
    only.
    """
    validate_origin(origin)
    repo = dataset.get_entities()
    now = last_seen or datetime.now(timezone.utc)
    buffer = RowBuffer()
    for data in logged_items(
        rows, "Import", item_name="Statement", logger=dataset._log, chunk_size=100_000
    ):
        buffer.add(
            statement_row_unsafe(
                data, dataset.name, repo.shards, now=now, origin=origin
            )
        )
        if len(buffer) >= bulk_size:
            repo.write_rows(buffer.flush(), batch_size=None)
    if buffer:
        repo.write_rows(buffer.flush(), batch_size=None)
