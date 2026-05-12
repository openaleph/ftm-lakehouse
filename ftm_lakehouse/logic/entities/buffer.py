from datetime import datetime
from typing import Generator, TypeAlias

from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.util import ensure_entity

from ftm_lakehouse.core.conventions.path import entity_shard

# Entities are never namespaced in ftm-lakehouse
namespace = Namespace()

# (shard, stmt, deleted_at)
StatementData: TypeAlias = tuple[str, Statement, datetime | None]

# {stmt_id: (shard, stmt, deleted_at)}
Buffer: TypeAlias = dict[str, tuple[str, Statement, datetime | None]]


class EntityBuffer:
    """Buffer statements keyed by statement ID, ordered by shard on flush."""

    def __init__(self, dataset: str, shards: int, origin: str | None = None) -> None:
        self.dataset: str = dataset
        self.shards: int = shards
        self.origin: str = origin or DEFAULT_ORIGIN
        self._buffer: Buffer = {}
        self._buffer_size: int = 0

    def add_statement(
        self, stmt: Statement, deleted_at: datetime | None = None
    ) -> None:
        """Add a statement to the buffer.

        When deleted_at is set, the statement is marked as a tombstone.
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

        if not stmt.id:
            raise RuntimeError("Missing statement ID!")

        shard = entity_shard(stmt.entity_id, self.shards)
        self._buffer[stmt.id] = (shard, stmt, deleted_at)
        self._buffer_size += 1

    def add_entity(self, e: EntityProxy, origin: str | None = None) -> None:
        entity = namespace.apply(e)
        entity = ensure_entity(e, StatementEntity, self.dataset)
        for stmt in entity.statements:
            stmt.origin = origin or self.origin or stmt.origin
            self.add_statement(stmt)

    def flush_buffer(self) -> Generator[StatementData, None, None]:
        """Yield (shard, stmt, deleted_at) sorted by shard."""
        for shard, stmt, deleted_at in sorted(
            self._buffer.values(), key=lambda v: v[0]
        ):
            yield shard, stmt, deleted_at
        self._buffer = {}
        self._buffer_size = 0
