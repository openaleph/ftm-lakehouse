"""JournalStore - SQL or http api statement buffer for write-ahead logging."""

from typing import Generator, Generic, Self, TypeAlias, TypeVar

import pyarrow as pa
from anystore.logging import get_logger
from rigour.time import utc_now

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
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
"""Stream of journal rows in the parquet statement schema.

The journal buffers exactly the rows the parquet store persists
(:data:`~ftm_lakehouse.model.statement.SHARDED_SCHEMA`), so a flush moves
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
    """

    def __init__(self, store: S, shards: int, origin: str | None = None) -> None:
        super().__init__(store.dataset, shards, origin)
        self.store = store

    def _insert(self, batch: pa.Table) -> None:
        """Write one :data:`SHARDED_SCHEMA` table to the journal."""
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
        and every column is in place. Only ``shard`` is re-derived – from
        ``entity_id`` against *this* dataset's shard count – so a client with
        a stale shard config cannot mis-route a partition.

        Args:
            batch: Rows carrying at least the :data:`SHARDED_SCHEMA` columns;
                extra columns are dropped and types are cast to the schema.

        Raises:
            KeyError: If a :data:`SHARDED_SCHEMA` column is missing.
            pyarrow.ArrowInvalid: If a column cannot be cast to its schema type.
        """
        if not batch.num_rows:
            return
        batch = batch.select(SHARDED_SCHEMA.names).cast(SHARDED_SCHEMA)
        batch = batch.set_column(
            SHARDED_SCHEMA.get_field_index("shard"), "shard", self._shard_column(batch)
        )
        self._insert(batch)

    def _shard_column(self, batch: pa.Table) -> pa.Array:
        if self.shards <= 1:
            # `entity_shard`'s single-shard sentinel, without the row loop
            return pa.repeat(entity_shard("", self.shards), batch.num_rows)
        return pa.array(
            [entity_shard(e, self.shards) for e in batch["entity_id"].to_pylist()],
            pa.string(),
        )

    def flush(self) -> None:
        """Insert the buffered statements."""
        if self._buffer_size:
            self._insert(statements_to_arrow(self.flush_buffer(), utc_now()))

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

    _is_api: bool = False
    """Overridden by the api store's mixin – see :func:`no_api`."""

    def __init__(
        self,
        dataset: str,
        uri: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.uri = uri or settings.resolved_journal_uri

    def writer(self, shards: int, origin: str | None = None) -> W:
        """Get a bulk writer for adding rows.

        Args:
            shards: The dataset's shard count. Callers resolve it from the
                dataset's config
            origin: Origin tag for statements written through this writer.
        """
        return self._writer_cls(self, shards=shards, origin=origin)

    @no_api
    def flush_batches(self, ordered: bool = True) -> StatementTables:
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

        Args:
            ordered: Sort each segment by ``shard``, so the consumer appends
                one parquet file per ``(shard, bucket, origin)`` partition.
                Single-shard datasets pass ``False`` and skip the sort.

        Yields:
            ``pa.RecordBatch`` in :data:`SHARDED_SCHEMA`.
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
