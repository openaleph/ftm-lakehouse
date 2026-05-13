from datetime import datetime

from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.util import ensure_entity

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.model.statement import StatementRow, StatementRows

# Entities are never namespaced in ftm-lakehouse
namespace = Namespace()


class EntityBuffer:
    """In-memory shard-sorted statement buffer.

    Keys statements by their statement id (deduplicating re-emissions in a
    single batch), then yields them sorted by shard on
    :meth:`flush_buffer` so the consumer (typically
    :meth:`EntityRepository.write_statements`) can accumulate per-shard
    parquet batches with bounded memory.
    """

    def __init__(self, dataset: str, shards: int, origin: str | None = None) -> None:
        self.dataset: str = dataset
        self.shards: int = shards
        self.origin: str = origin or DEFAULT_ORIGIN
        self._buffer: dict[str, StatementRow] = {}
        self._buffer_size: int = 0

    def add_statement(
        self, stmt: Statement, deleted_at: datetime | None = None
    ) -> None:
        """Add a statement to the buffer.

        Args:
            stmt: The FtM ``Statement`` to buffer. ``entity_id`` and ``id``
                are required; otherwise the call is a no-op.
            deleted_at: Tombstone marker. When set, the statement is queued
                as a delete in the parquet store.
        """
        if stmt.entity_id is None or stmt.id is None:
            return

        canonical_id = stmt.canonical_id or stmt.entity_id
        origin = stmt.origin or self.origin

        # Create new Statement with correct values (Statement is immutable)
        stmt = Statement(
            id=stmt.id,
            entity_id=stmt.entity_id,
            canonical_id=canonical_id,
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
        )

        shard = entity_shard(stmt.entity_id, self.shards)
        self._buffer[stmt.id] = StatementRow(shard, stmt, deleted_at)
        self._buffer_size += 1

    def add_entity(self, e: EntityProxy, origin: str | None = None) -> None:
        entity = namespace.apply(e)
        entity = ensure_entity(e, StatementEntity, self.dataset)
        for stmt in entity.statements:
            stmt.origin = origin or self.origin or stmt.origin
            stmt.first_seen = stmt.first_seen or entity.first_seen or entity.last_change
            stmt.last_seen = stmt.last_seen or entity.last_seen or entity.last_change
            self.add_statement(stmt)

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
        return bool(len(self))
