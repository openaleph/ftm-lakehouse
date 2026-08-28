"""JournalStore - SQL or http api statement buffer for write-ahead logging."""

from typing import Generator, Generic, Self, TypeAlias, TypeVar

import pyarrow as pa
from anystore.logging import get_logger
from rigour.time import utc_now

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import (
    JOURNAL_SCHEMA,
    LakehouseStatements,
    statements_to_arrow,
)

settings = Settings()
log = get_logger(__name__)

WRITE_BATCH_SIZE = 10_000

DRAIN_BATCH_SIZE = 1_000_000
"""Rows per table handed to a flush consumer."""

RecordBatches: TypeAlias = Generator[pa.RecordBatch, None, None]
"""What a reader hands over, on its way into a table."""

StatementTables: TypeAlias = Generator[pa.Table, None, None]
"""Stream of journal rows in the producer statement schema.

The journal buffers exactly the rows producers pack
(:data:`~ftm_lakehouse.model.statement.JOURNAL_SCHEMA`), so a flush moves
Arrow tables from one store to the other. ``pa.Table`` because that is what
every end of the pipe already speaks – ``statements_to_arrow``,
``ParquetStore.append``, ``adbc_ingest``, :meth:`BaseJournalWriter.add_batch`
– so nothing has to be taken apart and put back together on the way.
"""


S = TypeVar("S", bound="BaseJournalStore")


class BaseJournalWriter(EntityBuffer, Generic[S]):
    """
    Bulk writer for the journal.

    Not intended for direct use - use JournalStore.writer() instead.

    :meth:`add_statement` and :meth:`add_entity` buffer through
    :class:`EntityBuffer` – which re-keys statement ids and collapses
    re-emissions within the batch – and insert every
    :data:`WRITE_BATCH_SIZE` rows. :meth:`add_batch` writes an
    already-packed arrow table straight through.

    The buffer is constructed single-shard: journal rows carry no ``shard``
    column, so :class:`EntityBuffer`'s shard grouping would only reorder rows
    on their way into a SQL heap.
    """

    def __init__(self, store: S, origin: str | None = None) -> None:
        super().__init__(store.dataset, 1, origin)
        self.store = store

    def _insert(self, batch: pa.Table) -> None:
        """Write one :data:`JOURNAL_SCHEMA` table to the journal."""
        raise NotImplementedError

    def _insert_if_full(self) -> None:
        """Insert once the buffer holds a full batch.

        Called after a whole added item, never mid-entity:
        :meth:`EntityBuffer.add_entity` buffers through
        :meth:`EntityBuffer._add`, so this hook cannot fire between an
        entity's properties and the ``BASE_ID`` checksum row that closes it
        – inserts commit per batch, and a half entity flushed to parquet
        would survive merge.
        """
        if self._buffer_size >= WRITE_BATCH_SIZE:
            self.flush()

    def add_statement(self, *args, **kwargs) -> str | None:
        stmt_id = super().add_statement(*args, **kwargs)
        self._insert_if_full()
        return stmt_id

    def add_entity(self, *args, **kwargs) -> None:
        super().add_entity(*args, **kwargs)
        self._insert_if_full()

    def add_batch(self, batch: pa.Table) -> None:
        """Insert an already-packed Arrow table as-is – no repacking.

        Fast path for the api bulk route: the sending writer produced these
        rows through this same class, so statement ids are already re-keyed
        and every column is in place. Nothing about the dataset's layout is
        taken from the client – there is no shard key to take, and the one
        that ends up in parquet is derived at
        :meth:`~ftm_lakehouse.storage.parquet.ParquetStore.append`.

        Args:
            batch: Rows carrying at least the :data:`JOURNAL_SCHEMA` columns;
                extra columns are dropped and types are cast to the schema.

        Raises:
            KeyError: If a :data:`JOURNAL_SCHEMA` column is missing.
            pyarrow.ArrowInvalid: If a column cannot be cast to its schema type.
        """
        if not batch.num_rows:
            return
        self._insert(batch.select(JOURNAL_SCHEMA.names).cast(JOURNAL_SCHEMA))

    def flush(self) -> None:
        """Insert the buffered statements.

        The packed table's row count is the guard, not the buffer's – an
        empty insert is a wasted round trip on postgres and the api, and
        SQLAlchemy turns the sqlite one into ``INSERT ... DEFAULT VALUES``,
        which the journal's ``NOT NULL`` columns reject.
        """
        batch = statements_to_arrow(self.flush_buffer(), utc_now())
        if batch.num_rows:
            self._insert(batch)

    def rollback(self) -> None:
        """Drop the buffered statements that have not been inserted yet.

        Inserts commit per batch, so earlier batches of the same writer stay –
        harmless in an append-only journal, where re-emissions accumulate and
        ``merge`` collapses them.
        """
        self._buffer.clear()
        self._buffer_size = 0

    def close(self) -> None:
        """Close the connection."""
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        # the tail flush is where every writer under `WRITE_BATCH_SIZE` rows
        # sends its data, so it failing is the common exit, not an edge – and
        # its connection has to go back either way
        try:
            if exc_type is not None:
                self.rollback()
            else:
                self.flush()
        finally:
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

    _is_api: bool = False
    """Overridden by the api store's mixin – see :func:`no_api`."""

    def __init__(
        self,
        dataset: str,
        uri: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.uri = uri or settings.resolved_journal_uri

    def writer(self, origin: str | None = None) -> W:
        """Get a bulk writer for adding rows.

        Args:
            origin: Origin tag for statements written through this writer.
        """
        return self._writer_cls(self, origin=origin)

    @no_api
    def flush_batches(self) -> StatementTables:
        """Destructively iterate journal rows as Arrow batches.

        Local-only: draining a remote journal into a local parquet store is
        not a supported combination – the server that owns the journal owns
        its flush, and ``ApiEntityRepository.flush()`` delegates the whole
        thing there.

        Rows are discarded only after the consumer has written them: the
        implementation claims what is currently in the journal, hands it over
        whole batch by whole batch, and drops each claimed segment once the
        consumer comes back for more. A consumer that raises or abandons the
        generator leaves the rest in place for the next call – no
        transaction, and never a silent loss.

        Yields:
            ``pa.Table`` in :data:`JOURNAL_SCHEMA`.
        """
        raise NotImplementedError

    @no_api
    def iterate_entity(self, entity_id: str) -> LakehouseStatements:
        """Iterate the live (non-tombstone) journal statements of one entity.

        Non-destructive read used by the delete paths, which need the rows
        that are buffered but not yet in parquet. Not available on the api
        journal.
        """
        raise NotImplementedError

    def count(self) -> int:
        """Count rows for this dataset."""
        raise NotImplementedError

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        raise NotImplementedError
