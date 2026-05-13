"""JournalStore - SQL or http api statement buffer for write-ahead logging."""

from datetime import datetime
from typing import Generator, Generic, NamedTuple, Self, TypeAlias, TypeVar

from anystore.logging import get_logger

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import pack_statement, unpack_statement
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import StatementRow, StatementRows

settings = Settings()
log = get_logger(__name__)

WRITE_BATCH_SIZE = 10_000


class JournalRow(NamedTuple):
    """A single journal row — used for both SQL storage and wire format.

    ``shard`` is the entity-id hash bucket the statement routes to in the
    parquet store. PyArrow handles the final sort within each batch.
    """

    id: str
    shard: str
    data: str
    deleted_at: datetime | None


JournalRows: TypeAlias = Generator[JournalRow, None, None]


S = TypeVar("S", bound="BaseJournalStore")


class BaseJournalWriter(EntityBuffer, Generic[S]):
    """
    Bulk writer for the journal with batched upserts.

    Not intended for direct use - use JournalStore.writer() instead.
    """

    def __init__(self, store: S, shards: int, origin: str | None = None) -> None:
        super().__init__(store.dataset, shards, origin)
        self.store = store

    def _upsert_batch(self) -> None:
        raise NotImplementedError

    def flush_rows(self) -> JournalRows:
        for row in self.flush_buffer():
            yield JournalRow(
                row.stmt.id,
                row.shard,
                pack_statement(row.stmt),
                row.deleted_at,
            )

    def add_statement(self, *args, **kwargs) -> None:
        super().add_statement(*args, **kwargs)
        if self._buffer_size >= WRITE_BATCH_SIZE:
            self._upsert_batch()

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
        self.uri = uri or settings.resolved_journal_uri

    def writer(self, shards: int | None = None, origin: str | None = None) -> W:
        """Get a bulk writer for adding rows."""
        if shards is None:
            shards = settings.entity_shards
        return self._writer_cls(self, shards=shards, origin=origin)

    def iterate(self, *args, **kwargs) -> JournalRows:
        """Iterate all rows for this dataset, ordered by ``shard``.

        Yields:
            JournalRow(id, shard, data, deleted_at)
        """
        raise NotImplementedError

    def flush(self) -> JournalRows:
        """Iterate and delete all rows for this dataset atomically.

        This is a destructive read - rows are deleted after being yielded.
        If the consumer raises an exception, the transaction is rolled back.

        Yields raw :class:`JournalRow` (``data`` still packed). The
        HTTP-forwarding API uses this to stream JSONL without
        unpack-then-repack overhead; the parquet write path uses
        :meth:`flush_statements` instead.
        """
        raise NotImplementedError

    def flush_statements(self) -> StatementRows:
        """Destructively iterate as :class:`StatementRow` (data unpacked).

        Thin wrapper over :meth:`flush` for consumers (notably
        ``EntityRepository.flush``) that want :class:`Statement` objects
        instead of the packed wire format.
        """
        for r in self.flush():
            yield StatementRow(r.shard, unpack_statement(r.data), r.deleted_at)

    def count(self) -> int:
        """Count rows for this dataset."""
        raise NotImplementedError

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        raise NotImplementedError
