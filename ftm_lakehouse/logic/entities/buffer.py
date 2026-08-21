from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, Generator

import pyarrow as pa
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from followthemoney.statement.util import BASE_ID
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import LakeStatement
from ftmq.util import ensure_entity
from rigour.time import utc_now

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.helpers.statements import make_base_id_statement
from ftm_lakehouse.model.statement import (
    LakehouseStatement,
    LakehouseStatements,
    statements_to_arrow,
)
from ftm_lakehouse.util import validate_origin

settings = Settings()

# Entities are never namespaced in ftm-lakehouse
namespace = Namespace()


class EntityBuffer:
    """In-memory shard-grouped statement buffer.

    Keys statements by :attr:`~ftmq.store.lake.LakeStatement.dedupe_key`
    (``id``, ``origin``, ``fragment``) – the store's per-origin row identity:
    re-emissions in a single batch deduplicate, while the same id under
    distinct fragments *or* distinct origins stays distinct (merge never
    crosses origin partitions, so collapsing across origins here would
    silently drop provenance).

    Rows are held grouped by ``shard`` and drained one shard at a time, so
    :meth:`flush_tables` hands the parquet store one table per shard – one
    file per ``(shard, bucket, origin)`` partition instead of one per
    partition *per batch*.

    The buffer is bounded by ``max_rows`` (defaulting to
    :attr:`Settings.max_buffer_rows`, i.e. ``LAKEHOUSE_MAX_BUFFER_ROWS``) –
    the only memory bound on the write path, so a tenant colliding every
    entity id onto one shard still cannot grow it past the cap. Adding past
    the cap raises :class:`BufferFullError`; callers must flush (e.g. via
    :meth:`flush_tables` + ``write_batches``) and retry.
    """

    def __init__(
        self,
        dataset: str,
        shards: int,
        origin: str | None = None,
        max_rows: int | None = None,
        last_seen: datetime | None = None,
    ) -> None:
        self.dataset: str = dataset
        self.shards: int = shards
        self.origin: str = validate_origin(origin or DEFAULT_ORIGIN)
        self.max_rows: int = (
            max_rows if max_rows is not None else settings.max_buffer_rows
        )
        # Default emission timestamp for entities that carry none of their
        # own (:meth:`add_entity` pin chain) - e.g. the CLI ``--last-seen``.
        self.last_seen: str | None = last_seen.isoformat() if last_seen else None
        self._buffer: DefaultDict[str, dict[str, LakehouseStatement]] = defaultdict(
            dict
        )
        self._buffer_size: int = 0

    def _check_capacity(self) -> None:
        if self._buffer_size >= self.max_rows:
            raise BufferFullError(
                f"EntityBuffer is full ({self._buffer_size} rows, "
                f"max {self.max_rows}); flush before adding more"
            )

    def add_statement(
        self,
        stmt: Statement,
        deleted_at: datetime | None = None,
        fragment: str | None = None,
        origin: str | None = None,
    ) -> str | None:
        """Add a statement to the buffer.

        Subclasses hook here to react to a single added statement – the
        journal writer inserts a full batch. :meth:`add_entity` deliberately
        does *not* route through this method (see :meth:`_add`).

        The statement ``id`` is always re-derived – content-hashed under
        the *buffer's* dataset via :meth:`Statement.generate_key` – so the
        same content lands under the same id regardless of the payload's
        dataset context or a carried-over id. Identical content therefore
        collapses in :meth:`ParquetStore.merge` across imports, exports and
        round-trips.

        Args:
            stmt: The FtM ``Statement`` to buffer. ``entity_id``, ``prop``
                and ``value`` are required; otherwise the call is a no-op.
            deleted_at: Tombstone marker. When set, the statement is queued
                as a delete in the parquet store.
            fragment: Supersession group key. When set, a later emission of
                the same ``(entity_id, prop, fragment)`` replaces this
                statement. ``None`` (the default) preserves the fragment of
                a passed :class:`ftmq.store.lake.LakeStatement` and means
                non-fragment (empty-string sentinel) otherwise.
            origin: Origin tag override. Falls back to ``stmt.origin`` then
                the buffer's default origin.

        Returns:
            The (re-keyed) statement id, or ``None`` if the statement was
            skipped.

        Raises:
            ValueError: If the resolved origin is not a safe origin name
                (see :func:`ftm_lakehouse.util.validate_origin`).
            BufferFullError: If the buffer has reached :attr:`max_rows`
                and has not been flushed.
        """
        return self._add(stmt, deleted_at, fragment, origin)

    def _add(
        self,
        stmt: Statement,
        deleted_at: datetime | None,
        fragment: str | None,
        origin: str | None,
    ) -> str | None:
        """Buffer one statement – the shared implementation.

        :meth:`add_entity` routes here rather than through
        :meth:`add_statement` so a subclass hooking the public method cannot
        fire mid-entity: the journal writer inserts per batch, and a batch
        boundary inside one entity would land its properties without the
        ``BASE_ID`` checksum row that closes it.
        """
        if stmt.entity_id is None:
            return None
        self._check_capacity()

        origin = validate_origin(origin or stmt.origin or self.origin)
        if fragment is None and isinstance(stmt, LakeStatement):
            fragment = stmt.fragment

        # Create new LakehouseStatement with correct values (Statement is immutable).
        # ``id`` is deliberately unset so the constructor content-hashes it
        # under ``self.dataset``. canonical_id is intentionally unset –
        # storage drops it and FtM defaults it to entity_id (single-dataset
        # store, no resolution).
        stmt = LakehouseStatement(
            entity_id=stmt.entity_id,
            prop=stmt.prop,
            schema=stmt.schema,
            value=stmt.value,
            dataset=self.dataset,
            lang=stmt.lang,
            original_value=stmt.original_value,
            external=stmt.external,
            first_seen=stmt.first_seen,
            last_seen=stmt.last_seen,
            origin=origin,
            fragment=fragment,
            shard=entity_shard(stmt.entity_id, self.shards),
            deleted_at=deleted_at,
        )
        if stmt.id is None:
            return None

        self._buffer[stmt.shard][stmt.dedupe_key] = stmt
        self._buffer_size += 1
        return stmt.id

    def add_entity(
        self,
        e: EntityProxy,
        origin: str | None = None,
        fragment: str | None = None,
    ) -> None:
        """Add an entity's statements to the buffer.

        Args:
            e: The entity whose statements to buffer.
            origin: Origin tag override for this entity's statements.
                ``None`` keeps each statement's own origin (a read-back
                ``StatementEntity`` carries per-statement provenance),
                falling back to the buffer's default origin.
            fragment: Supersession group key for this emission. A later
                ``add_entity`` with the same fragment replaces the earlier
                emission per ``(entity_id, prop, fragment)`` group.

        Raises:
            BufferFullError: If the buffer is at capacity before this
                entity's statements are added. Callers should flush and
                retry; partial entities are never buffered.
            ValueError: If ``origin`` is set but not a safe origin name.
        """
        if origin is not None:
            validate_origin(origin)
        self._check_capacity()
        e = namespace.apply(e)
        e = ensure_entity(e, StatementEntity, self.dataset)
        # Producer contract: all rows of one *fragment* emission share a
        # single last_seen – sub-second jitter (or heterogeneous
        # per-statement timestamps from a store read-back) would break the
        # tie that lets multi-valued props survive supersession together,
        # so fragment emissions pin one timestamp: the entity's own, else
        # the buffer default, else now. Non-fragment statements keep their
        # own last_seen (faithful provenance on round-trips) and only fall
        # back to the pinned value when unset.
        last_seen = (
            e.last_seen or e.last_change or self.last_seen or utc_now().isoformat()
        )
        ids: set[str] = set()
        first_seens: set[str] = set()
        last_seens: set[str] = set()
        for stmt in e.statements:
            if stmt.prop == BASE_ID:
                # Skip FtM's synthesized checksum statement: its value is
                # hashed over the *incoming* statement ids, but
                # add_statement re-keys every id under the lakehouse
                # dataset - the checksum is re-derived below over the
                # re-keyed ids so identical content collapses regardless
                # of the payload's dataset context.
                continue
            stmt.first_seen = stmt.first_seen or e.first_seen or e.last_change
            stmt.last_seen = last_seen if fragment else (stmt.last_seen or last_seen)
            # origin resolution delegates to _add:
            # override > stmt.origin > buffer default
            stmt_id = self._add(stmt, None, fragment, origin)
            if stmt_id is None:
                continue
            ids.add(stmt_id)
            if stmt.first_seen:
                first_seens.add(stmt.first_seen)
            if stmt.last_seen:
                last_seens.add(stmt.last_seen)
        if e.id is None:
            return
        base = make_base_id_statement(
            self.dataset,
            e.id,
            e.schema.name,
            ids,
            first_seen=e.last_change or min(first_seens, default=None),
            last_seen=(
                last_seen if fragment else (max(last_seens, default=None) or last_seen)
            ),
        )
        self._add(base, None, fragment, origin)

    def _drain(self) -> Generator[list[LakehouseStatement], None, None]:
        """Pop the buffer one shard at a time, in insertion order.

        Rows leave the buffer as they are handed over, so a consumer that
        abandons the generator keeps the shards it never saw instead of
        losing them (or seeing them twice on the next drain).
        """
        for shard in list(self._buffer):
            rows = self._buffer.pop(shard)
            self._buffer_size -= len(rows)
            yield list(rows.values())

    def flush_buffer(self) -> LakehouseStatements:
        """Drain the buffer as a flat statement stream, shard by shard.

        Yields:
            :class:`LakehouseStatement` grouped by ``shard``.
        """
        for rows in self._drain():
            yield from rows

    def flush_tables(
        self, now: datetime | None = None
    ) -> Generator[pa.Table, None, None]:
        """Drain the buffer as one packed Arrow table per shard.

        What the parquet write path consumes: each table is scoped to a
        single ``shard``, so
        :meth:`~ftm_lakehouse.repository.entities.EntityRepository.write_batches`
        appends one parquet file per ``(shard, bucket, origin)`` partition it
        spans. Peak memory is one shard's rows, bounded by :attr:`max_rows`.

        Args:
            now: Default timestamp for statements missing ``first_seen`` /
                ``last_seen``. Defaults to the current UTC time.

        Yields:
            Tables in :data:`~ftm_lakehouse.model.statement.SHARDED_SCHEMA`.
        """
        now = now or utc_now()
        for rows in self._drain():
            yield statements_to_arrow(rows, now)

    def __len__(self) -> int:
        return self._buffer_size

    def __bool__(self) -> bool:
        return self._buffer_size > 0
