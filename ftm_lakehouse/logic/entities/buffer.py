from collections import defaultdict
from datetime import datetime
from typing import Generator, TypeAlias

from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import get_schema_bucket
from ftmq.util import ensure_entity

namespace = Namespace()

# bucket, origin, canonical_id, stmt, deleted_at
StatementData: TypeAlias = tuple[str, str, str, Statement, datetime | None]

# {bucket: {origin: {canonical_id: {stmt_id: (stmt, deleted_at)}}}}
Buffer: TypeAlias = defaultdict[
    str,
    defaultdict[str, defaultdict[str, dict[str, tuple[Statement, datetime | None]]]],
]


def _make_buffer() -> Buffer:
    return defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))


class EntityBuffer:
    """
    Buffers statements, tracks deleted_at and yields sorted by bucket, origin,
    entity_id to match deltalake store partitions.
    """

    def __init__(self, dataset: str, origin: str | None = None) -> None:
        self.dataset: str = dataset
        self.origin: str = origin or DEFAULT_ORIGIN
        self._buffer: Buffer = _make_buffer()
        self._buffer_size: int = 0

    def add_statement(
        self, stmt: Statement, deleted_at: datetime | None = None
    ) -> None:
        """
        Add a statement to the writers buffer. When deleted_at is set, the
        statement is marked as deleted for the consumers.
        """
        if stmt.entity_id is None or stmt.id is None:
            return

        canonical_id = stmt.canonical_id or stmt.entity_id
        origin = stmt.origin or self.origin
        bucket = get_schema_bucket(stmt.schema)

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

        # should not happen:
        if not stmt.id or stmt.id is None:
            raise RuntimeError("Missing statement ID!")

        self._buffer[bucket][origin][canonical_id][stmt.id] = (
            stmt,
            deleted_at,
        )
        self._buffer_size += 1

    def add_entity(self, e: EntityProxy, origin: str | None = None) -> None:
        entity = namespace.apply(e)
        entity = ensure_entity(e, StatementEntity, self.dataset)
        for stmt in entity.statements:
            stmt.origin = origin or self.origin or stmt.origin
            self.add_statement(stmt)

    def flush_buffer(self) -> Generator[StatementData, None, None]:
        for bucket in sorted(self._buffer):
            for origin in sorted(self._buffer[bucket]):
                for id in sorted(self._buffer[bucket][origin]):
                    for stmt, deleted_at in self._buffer[bucket][origin][id].values():
                        yield bucket, origin, id, stmt, deleted_at
        self._buffer = _make_buffer()
        self._buffer_size = 0
