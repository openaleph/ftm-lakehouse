"""JournalStore - SQL or http api statement buffer for write-ahead logging."""

from datetime import datetime
from typing import Generator, Generic, NamedTuple, Self, TypeAlias, TypeVar

from anystore.logging import get_logger
from ftmq.store.lake import LakeStatement

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import MalformedStatementError
from ftm_lakehouse.helpers.statements import (
    UNIT_SEP,
    UNPACK_MIN_FIELDS,
    pack_journal_row,
    unpack_journal_row,
)
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import StatementRow, StatementRows

settings = Settings()
log = get_logger(__name__)

WRITE_BATCH_SIZE = 10_000


class JournalRow(NamedTuple):
    """A single journal row – used for both SQL storage and wire format.

    ``shard`` is the entity-id hash bucket the statement routes to in the
    parquet store. PyArrow handles the final sort within each batch.
    ``fragment`` is the supersession group key (empty string = non-fragment);
    it rides as its own column – not inside the packed ``data`` – because it
    is part of the journal's primary key. Writers derive it from
    ``LakeStatement.fragment``; readers stamp it back via
    :meth:`ftmq.store.lake.LakeStatement.from_statement`.
    """

    id: str
    shard: str
    data: str
    deleted_at: datetime | None
    fragment: str = ""


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
        self._raw_rows: dict[tuple[str, str], JournalRow] = {}

    def _upsert_batch(self) -> None:
        raise NotImplementedError

    def _pending(self) -> int:
        """Rows waiting for the next upsert batch, across both add paths."""
        return self._buffer_size + len(self._raw_rows)

    def flush_rows(self) -> JournalRows:
        yield from self._raw_rows.values()
        self._raw_rows = {}
        for row in self.flush_buffer():
            yield JournalRow(
                row.stmt.id,
                row.shard,
                pack_journal_row(row.stmt),
                row.deleted_at,
                row.stmt.fragment,
            )

    def add_statement(self, *args, **kwargs) -> str | None:
        stmt_id = super().add_statement(*args, **kwargs)
        if self._pending() >= WRITE_BATCH_SIZE:
            self._upsert_batch()
        return stmt_id

    def add_row(self, row: JournalRow) -> None:
        """Buffer an already-packed journal row as-is – no unpack / re-pack.

        Fast path for the api bulk route: the sending writer produced the row
        through this same class, so the statement id is already re-keyed and
        ``data`` already packed. Only ``shard`` is re-derived – from the
        packed ``entity_id`` against *this* dataset's shard count – so a
        client with a stale shard config cannot mis-route a partition. Rows
        deduplicate per ``(id, fragment)`` within a batch (latest wins), the
        same key the upsert conflicts on. Do not mix with
        :meth:`add_statement` in one writer – a key present in both buffers
        would reach a single multi-values upsert twice.

        Raises:
            MalformedStatementError: If ``row.data`` has fewer than
                :data:`~ftm_lakehouse.helpers.statements.UNPACK_MIN_FIELDS`
                fields.
        """
        parts = row.data.split(UNIT_SEP, UNPACK_MIN_FIELDS)
        if len(parts) < UNPACK_MIN_FIELDS:
            raise MalformedStatementError(
                f"Packed statement has {len(parts)} fields; "
                f"expected at least {UNPACK_MIN_FIELDS}"
            )
        shard = entity_shard(parts[1], self.shards)
        self._raw_rows[(row.id, row.fragment)] = row._replace(shard=shard)
        if self._pending() >= WRITE_BATCH_SIZE:
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

    def writer(self, shards: int, origin: str | None = None) -> W:
        """Get a bulk writer for adding rows.

        Args:
            shards: The dataset's shard count. Callers resolve it from the
                dataset's config
            origin: Origin tag for statements written through this writer.
        """
        return self._writer_cls(self, shards=shards, origin=origin)

    def flush(self) -> JournalRows:
        """Destructively iterate journal rows in raw packed form.

        Rows are deleted after being yielded. If the consumer raises an
        exception the transaction is rolled back.

        The HTTP-forwarding journal API uses this to stream JSONL without
        unpack-then-repack overhead; the parquet write path uses
        :meth:`flush_statements` instead.

        Yields:
            :class:`JournalRow` ``(id, shard, data, deleted_at)`` – ``data``
            is still packed (the unit-separator-delimited statement wire
            format).
        """
        raise NotImplementedError

    def flush_statements(self) -> StatementRows:
        """Destructively iterate as :class:`StatementRow` (data unpacked).

        Thin wrapper over :meth:`flush` for consumers (notably
        :meth:`EntityRepository.flush`) that want ``Statement`` objects
        instead of the packed wire format. Malformed rows (failed
        :func:`unpack_journal_row`) are logged and skipped so one corrupt
        row can't abort the whole flush.

        Yields:
            :class:`StatementRow` ``(shard, stmt, deleted_at)`` produced by
            unpacking each :class:`JournalRow`; ``stmt`` is a
            :class:`ftmq.store.lake.LakeStatement` stamped with the row's
            ``fragment`` column.
        """
        for r in self.flush():
            try:
                stmt = unpack_journal_row(r.data)
            except MalformedStatementError as exc:
                log.warning(
                    "Skipping malformed journal row",
                    row_id=r.id,
                    shard=r.shard,
                    error=str(exc),
                )
                continue
            yield StatementRow(
                r.shard, LakeStatement.from_statement(stmt, r.fragment), r.deleted_at
            )

    def count(self) -> int:
        """Count rows for this dataset."""
        raise NotImplementedError

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        raise NotImplementedError
