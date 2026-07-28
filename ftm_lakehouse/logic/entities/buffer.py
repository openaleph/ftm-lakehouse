from datetime import datetime, timezone

from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import LakeStatement
from ftmq.util import ensure_entity

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import BufferFullError
from ftm_lakehouse.model.statement import StatementRow, StatementRows
from ftm_lakehouse.util import validate_origin

settings = Settings()

# Entities are never namespaced in ftm-lakehouse
namespace = Namespace()


class EntityBuffer:
    """In-memory shard-sorted statement buffer.

    Keys statements by :attr:`LakeStatement.dedupe_key` (deduplicating
    re-emissions in a single batch; the same id under distinct fragments
    stays distinct), then yields them sorted by shard on
    :meth:`flush_buffer` so the consumer (typically
    :meth:`EntityRepository.write_statements`) can accumulate per-shard
    parquet batches with bounded memory.

    The buffer is bounded by ``max_rows`` (defaulting to
    :attr:`Settings.max_buffer_rows`, i.e. ``LAKEHOUSE_MAX_BUFFER_ROWS``).
    Adding past the cap raises :class:`BufferFullError`; callers must
    flush (e.g. via :meth:`flush_buffer` + ``write_statements``) and
    retry.
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
        self._buffer: dict[str, StatementRow] = {}
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
    ) -> None:
        """Add a statement to the buffer.

        Args:
            stmt: The FtM ``Statement`` to buffer. ``entity_id`` and ``id``
                are required; otherwise the call is a no-op.
            deleted_at: Tombstone marker. When set, the statement is queued
                as a delete in the parquet store.
            fragment: Supersession group key. When set, a later emission of
                the same ``(entity_id, prop, fragment)`` replaces this
                statement. ``None`` (the default) preserves the fragment of
                a passed :class:`ftmq.store.lake.LakeStatement` and means
                non-fragment (empty-string sentinel) otherwise.
            origin: Origin tag override. Falls back to ``stmt.origin`` then
                the buffer's default origin.

        Raises:
            ValueError: If the resolved origin is not a safe origin name
                (see :func:`ftm_lakehouse.util.validate_origin`).
            BufferFullError: If the buffer has reached :attr:`max_rows`
                and has not been flushed.
        """
        if stmt.entity_id is None or stmt.id is None:
            return
        self._check_capacity()

        origin = validate_origin(origin or stmt.origin or self.origin)
        if fragment is None and isinstance(stmt, LakeStatement):
            fragment = stmt.fragment

        # Create new LakeStatement with correct values (Statement is immutable).
        # canonical_id is intentionally unset – storage drops it and FtM
        # defaults it to entity_id (single-dataset store, no resolution).
        stmt = LakeStatement(
            id=stmt.id,
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
        )

        shard = entity_shard(stmt.entity_id, self.shards)
        self._buffer[stmt.dedupe_key] = StatementRow(shard, stmt, deleted_at)
        self._buffer_size += 1

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
            e.last_seen
            or e.last_change
            or self.last_seen
            or datetime.now(timezone.utc).isoformat()
        )
        for stmt in e.statements:
            stmt.first_seen = stmt.first_seen or e.first_seen or e.last_change
            stmt.last_seen = last_seen if fragment else (stmt.last_seen or last_seen)
            # origin resolution delegates to add_statement:
            # override > stmt.origin > buffer default
            self.add_statement(stmt, fragment=fragment, origin=origin)

    def flush_buffer(self) -> StatementRows:
        """Yield buffered rows sorted by shard, then clear the buffer.

        Yields:
            :class:`StatementRow` sorted by ``shard`` so the consumer can
            stream per-shard parquet batches with bounded memory.
        """
        for row in sorted(self._buffer.values(), key=lambda r: r.shard):
            yield row
        self._buffer = {}
        self._buffer_size = 0

    def __len__(self) -> int:
        return self._buffer_size

    def __bool__(self) -> bool:
        return self._buffer_size > 0
