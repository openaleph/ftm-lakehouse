"""JournalStore - SQL or http api statement buffer for write-ahead logging."""

from datetime import datetime
from typing import Generator, Generic, Self, TypeAlias, TypeVar, cast

from anystore.logging import get_logger
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.namespace import Namespace
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import get_schema_bucket
from ftmq.util import ensure_entity

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import pack_statement, pack_tombstone

settings = Settings()
log = get_logger(__name__)

WRITE_BATCH_SIZE = 10_000

JournalRow = tuple[
    str, str, str, str, str, datetime | None
]  # (id, bucket, origin, canonical_id, data)
JournalRows: TypeAlias = Generator[JournalRow, None, None]


S = TypeVar("S", bound="BaseJournalStore")


class BaseJournalWriter(Generic[S]):
    """
    Bulk writer for the journal with batched upserts.

    Not intended for direct use - use JournalStore.writer() instead.
    """

    def __init__(self, store: S, origin: str | None = None) -> None:
        self.store = store
        self.dataset = store.dataset
        self.origin = origin or DEFAULT_ORIGIN
        self.batch: list[dict] = []
        self.namespace = Namespace()

    def _upsert_batch(self) -> None:
        raise NotImplementedError

    def add(
        self,
        row_id: str,
        bucket: str,
        origin: str,
        canonical_id: str,
        data: str,
        deleted_at: datetime | None = None,
    ) -> None:
        """Add a raw row to the journal batch."""
        self.batch.append(
            {
                "id": row_id,
                "bucket": bucket,
                "origin": origin,
                "canonical_id": canonical_id,
                "data": data,
                "deleted_at": deleted_at,
            }
        )

        if len(self.batch) >= WRITE_BATCH_SIZE:
            self._upsert_batch()

    def add_statement(
        self, stmt: Statement, deleted_at: datetime | None = None
    ) -> None:
        """Add a statement to the journal.

        When deleted_at is set, the statement is packed as a tombstone
        (only routing fields, payload stripped).
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

        data = pack_tombstone(stmt) if deleted_at is not None else pack_statement(stmt)

        self.add(
            row_id=cast(str, stmt.id),
            bucket=get_schema_bucket(stmt.schema),
            origin=origin,
            canonical_id=canonical_id,
            data=data,
            deleted_at=deleted_at,
        )

    def add_entity(self, entity: EntityProxy) -> None:
        """Add all statements from an entity to the journal."""
        entity = self.namespace.apply(entity)
        entity = ensure_entity(entity, StatementEntity, self.dataset)
        for stmt in entity.statements:
            self.add_statement(stmt)

    def flush(self) -> None:
        """Flush pending rows and commit transaction."""
        self._upsert_batch()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        pass

    def close(self) -> None:
        """Close the connection."""
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if exc_type is not None:
            self.rollback()
        else:
            self.flush()
        self.close()


W = TypeVar("W", bound=BaseJournalWriter)


class BaseJournalStore(Generic[W]):
    """
    Journal for buffering statement writes.

    The journal is designed as a write-ahead log - data is written
    here first, then flushed to permanent parquet storage.

    Args:
        dataset: Dataset name (used for table name and filtering)
        uri: http api url or SQLAlchemy database uri
    """

    _writer_cls: type[W]

    def __init__(
        self,
        dataset: str,
        uri: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.uri = uri or settings.journal_uri

    def writer(self, origin: str | None = None) -> W:
        """Get a bulk writer for adding rows."""
        return self._writer_cls(self, origin=origin)

    def iterate(self) -> JournalRows:
        """
        Iterate all rows for this dataset, ordered for batch processing.

        Rows are ordered by (bucket, origin, canonical_id) for efficient
        partitioned writes to downstream storage.

        Yields:
            Tuples of (id, bucket, origin, canonical_id, data)
        """
        raise NotImplementedError

    def flush(self) -> JournalRows:
        """
        Iterate and delete all rows for this dataset atomically.

        This is a destructive read - rows are deleted after being yielded.
        If the consumer raises an exception, the transaction is rolled back.

        Yields:
            Tuples of (id, bucket, origin, canonical_id, data)
        """
        raise NotImplementedError

    def count(self) -> int:
        """Count rows for this dataset."""
        raise NotImplementedError

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        raise NotImplementedError
