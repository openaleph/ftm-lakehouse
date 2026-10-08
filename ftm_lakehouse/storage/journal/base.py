"""Journal store base – write-ahead statement buffer (SQL or http api)."""

from typing import Generator, Generic, Self, TypeAlias, TypeVar

import pyarrow as pa

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import JOURNAL_SCHEMA, LakehouseStatements

settings = Settings()

WRITE_BATCH_SIZE = 10_000

RecordBatches: TypeAlias = Generator[pa.RecordBatch, None, None]
"""Record batches a segment reader yields."""

StatementTables: TypeAlias = Generator[pa.Table, None, None]
"""Stream of journal rows as `JOURNAL_SCHEMA` tables – what a flush moves."""


S = TypeVar("S", bound="BaseJournalStore")


class BaseJournalWriter(EntityBuffer, Generic[S]):
    """Bulk journal writer – get one via `BaseJournalStore.writer`.

    `add_statement` / `add_entity` buffer through `EntityBuffer` and insert
    every `WRITE_BATCH_SIZE` rows; `add_batch` writes a packed table through.
    """

    def __init__(
        self, store: S, origin: str | None = None, role: str | None = None
    ) -> None:
        super().__init__(store.dataset, origin, role=role)
        self.store = store

    def _insert(self, batch: pa.Table) -> None:
        """Write one `JOURNAL_SCHEMA` table to the journal."""
        raise NotImplementedError

    def _insert_if_full(self) -> None:
        """Insert once the buffer holds a full batch.

        Only called after a whole item, so an entity is never split from its
        ``BASE_ID`` checksum row – a half entity in parquet would survive merge.
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
        """Insert an already-packed Arrow table as-is – the api bulk route's path.

        Extra columns are dropped and types cast to `JOURNAL_SCHEMA`; a missing
        column raises `KeyError`. No shard key is taken from the client.
        """
        if not batch.num_rows:
            return
        self._insert(batch.select(JOURNAL_SCHEMA.names).cast(JOURNAL_SCHEMA))

    def flush(self) -> None:
        """Insert the buffered statements.

        Guarded on the packed table's rows: an empty sqlite insert becomes
        ``INSERT ... DEFAULT VALUES``, which the ``NOT NULL`` columns reject.
        """
        batch = self.flush_table()
        if batch.num_rows:
            self._insert(batch)

    def rollback(self) -> None:
        """Drop the buffered statements not inserted yet.

        Batches already inserted stay committed – ``merge`` collapses re-emissions.
        """
        self.flush_buffer()

    def close(self) -> None:
        """Close the connection."""
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        # the tail flush is where most writers send their data – close even
        # when it fails
        try:
            if exc_type is not None:
                self.rollback()
            else:
                self.flush()
        finally:
            self.close()


W = TypeVar("W", bound=BaseJournalWriter)


class BaseJournalStore(Generic[W]):
    """Write-ahead journal – statements land here first, then flush to parquet."""

    _writer_cls: type[W]

    _is_api: bool = False
    """Overridden by the api store's mixin – see `no_api`."""

    def __init__(
        self,
        dataset: str,
        uri: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.uri = uri or settings.resolved_journal_uri

    def writer(self, origin: str | None = None, role: str | None = None) -> W:
        """Get a bulk writer; ``origin`` / ``role`` apply to what it writes."""
        return self._writer_cls(self, origin=origin, role=role)

    @no_api
    def flush_batches(self) -> StatementTables:
        """Destructively iterate journal rows as `JOURNAL_SCHEMA` tables.

        Rows are dropped only once the consumer comes back for more, so one that
        raises or abandons the generator leaves them for the next call.
        Local-only – in api mode the server flushes.
        """
        raise NotImplementedError

    @no_api
    def iterate_entity(self, entity_id: str) -> LakehouseStatements:
        """Iterate one entity's live (non-tombstone) journal rows – for deletes."""
        raise NotImplementedError

    def count(self) -> int:
        """Count rows for this dataset."""
        raise NotImplementedError

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        raise NotImplementedError
