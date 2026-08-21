"""JournalStore - SQL statement buffer for write-ahead logging."""

from __future__ import annotations

import random
import threading
import time
from binascii import crc32
from contextlib import contextmanager
from functools import cached_property
from typing import Any, Generator
from uuid import uuid4

import pyarrow as pa
from anystore.logging import get_logger
from ftmq.util import datetime_iso
from rigour.time import utc_now
from sqlalchemy import MetaData, Table, delete, insert, inspect, select
from sqlalchemy.engine import Engine, create_engine, make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool, StaticPool
from sqlalchemy.schema import CreateTable

from ftm_lakehouse.exceptions import ImproperlyConfigured
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    LakehouseStatement,
    LakehouseStatements,
    journal_table,
)
from ftm_lakehouse.storage.journal.base import (
    DRAIN_BATCH_SIZE,
    BaseJournalStore,
    BaseJournalWriter,
    RecordBatches,
    StatementTables,
)

try:  # optional `postgres` extra – ADBC does the Arrow row IO on postgres
    from adbc_driver_postgresql import dbapi as adbc_pg

    Connection = adbc_pg.Connection
except ImportError:  # pragma: no cover
    adbc_pg = None
    Connection = Any

log = get_logger(__name__)

READ_BATCH_SIZE = 10_000
"""Rows per cursor fetch when draining a segment through SQLAlchemy."""

SEGMENT_INFIX = "-seg-"
"""Separates a journal table from its rotated segments.

``-`` is illegal in dataset names (followthemoney's ``dataset_name_check``
allows lowercase alphanumerics and ``_``), so a segment name can never
collide with another dataset's journal table.
"""

ROTATE_LOCK_TIMEOUT = "5s"
ROTATE_MAX_RETRIES = 5
ROTATE_BASE_DELAY = 1  # seconds

COLUMNS = ", ".join(f'"{name}"' for name in SHARDED_SCHEMA.names)


def _row_to_statement(row: Any) -> LakehouseStatement:
    """Build a statement from a journal row – typed columns, no unpacking."""
    return LakehouseStatement(
        id=row.id,
        entity_id=row.entity_id,
        prop=row.prop,
        schema=row.schema,
        value=row.value,
        dataset=row.dataset,
        lang=row.lang,
        original_value=row.original_value,
        external=bool(row.external),
        first_seen=datetime_iso(row.first_seen),
        last_seen=datetime_iso(row.last_seen),
        origin=row.origin,
        fragment=row.fragment or "",
        shard=row.shard,
        deleted_at=row.deleted_at,
    )


class SqlJournalWriter(BaseJournalWriter["SqlJournalStore"]):
    """SQL-backed bulk writer, appending batches.

    Holds one connection for its lifetime and hands each packed batch to the
    store, whose dialect implementation owns the insert.
    """

    def __init__(
        self,
        store: "SqlJournalStore",
        shards: int,
        origin: str | None = None,
    ) -> None:
        super().__init__(store, shards=shards, origin=origin)
        self._conn: Any = None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self.store.connect()
        return self._conn

    def _insert(self, batch: pa.Table) -> None:
        self.store.insert_batch(self.conn, batch)

    def close(self) -> None:
        """Close the connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class SqlJournalStore(BaseJournalStore[SqlJournalWriter]):
    """
    SQL-based journal for buffering writes.

    An append-only heap per dataset, carrying the parquet statement columns
    (:data:`~ftm_lakehouse.model.statement.SHARDED_SCHEMA`). A flush claims
    the whole table by renaming it to a timestamped segment and creating a
    fresh one in the same DDL transaction, streams the segment out as Arrow,
    and drops it – so cleanup is a catalog operation, never a ``DELETE``.

    Dialect specifics live in the subclasses :class:`SqliteJournalStore` and
    :class:`PostgresJournalStore`, picked once by :func:`sql_journal` – the
    same construction-time choice ``get_journal`` makes for the api store.
    """

    _writer_cls = SqlJournalWriter

    lock_timeout: str | None = None
    """Dialect bound on how long the rotation waits for in-flight writers."""

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        super().__init__(dataset, uri)
        self.engine = self.make_engine()
        self.metadata = MetaData()
        self.table = journal_table(self.metadata, f"journal_{dataset}")
        self.metadata.create_all(self.engine, tables=[self.table], checkfirst=True)

    # -- dialect hooks

    def make_engine(self) -> Engine:
        raise NotImplementedError

    def connect(self) -> Any:
        """Open a connection for a writer's inserts."""
        raise NotImplementedError

    def insert_batch(self, conn: Any, batch: pa.Table) -> None:
        """Append one packed batch to the live table."""
        raise NotImplementedError

    def read_segment(self, name: str, ordered: bool) -> RecordBatches:
        """Stream a segment's rows, ``ORDER BY shard`` when asked."""
        raise NotImplementedError

    @contextmanager
    def flush_lock(self) -> Generator[bool, None, None]:
        """Hold this dataset's flush window, or report that someone else has it.

        Rotation alone does not serialize concurrent flushes: the second one
        finds the live table already empty, skips rotating, and drains the
        first one's segment – duplicating every row and then failing on the
        double ``DROP``. The lock must release itself when a flusher dies,
        or a crash would strand the segment it was draining.
        """
        raise NotImplementedError
        yield True  # pragma: no cover - typing

    def _set_lock_timeout(self, conn: Any) -> None:
        if self.lock_timeout is not None:
            conn.exec_driver_sql(f"SET LOCAL lock_timeout = '{self.lock_timeout}'")

    # -- segments

    @property
    def _prefix(self) -> str:
        return f"{self.table.name}{SEGMENT_INFIX}"

    def _segment_name(self) -> str:
        """A fresh segment name – time-ordered, unique against a racing flush."""
        return f"{self._prefix}{utc_now().strftime('%Y%m%dT%H%M%S')}{uuid4().hex[:4]}"

    def _segments(self) -> list[str]:
        """Rotated segments, oldest first – the timestamp name sorts for us.

        This is the whole of orphan recovery: a segment left behind by a
        crashed or abandoned flush is picked up by the next one.
        """
        names = inspect(self.engine).get_table_names()
        return sorted(n for n in names if n.startswith(self._prefix))

    def _table_names(self) -> list[str]:
        """The live table plus every un-dropped segment."""
        return [self.table.name, *self._segments()]

    def _table(self, name: str) -> Table:
        return journal_table(MetaData(), name)

    def _rotate(self) -> None:
        """Claim the current journal: rename it, create a fresh one, atomically.

        DDL is transactional in both dialects, so a writer sees either the
        old table or the new one, never a gap. The rename takes the strongest
        table lock, which conflicts with every in-flight insert – so it waits
        out uncommitted writers, and no row can land in the segment after it
        returns. A writer blocked on that lock re-resolves the table name and
        continues into the fresh table.

        Raises:
            RuntimeError: If the lock could not be taken within
                :data:`ROTATE_MAX_RETRIES` attempts.
        """
        name = self._segment_name()
        attempt = 0
        while True:
            try:
                with self.engine.begin() as conn:
                    self._set_lock_timeout(conn)
                    conn.exec_driver_sql(
                        f'ALTER TABLE "{self.table.name}" RENAME TO "{name}"'
                    )
                    conn.execute(CreateTable(self.table))
                return
            except OperationalError as exc:
                attempt += 1
                if attempt >= ROTATE_MAX_RETRIES:
                    raise RuntimeError(
                        f"Cannot rotate journal `{self.table.name}`: {exc}"
                    )
                delay = ROTATE_BASE_DELAY * 2**attempt + random.uniform(
                    0, ROTATE_BASE_DELAY
                )
                log.warning(
                    "Journal rotation blocked, retrying in %.2fs (attempt %d)",
                    delay,
                    attempt,
                )
                time.sleep(delay)

    def _drop(self, name: str) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')

    def _has_rows(self, name: str) -> bool:
        with self.engine.connect() as conn:
            res = conn.exec_driver_sql(f'SELECT 1 FROM "{name}" LIMIT 1')
            return res.first() is not None

    # -- flush

    def flush_batches(self, ordered: bool = True) -> StatementTables:
        """Rotate the journal, then stream each segment as Arrow.

        Held under :meth:`flush_lock` for the whole window – a second flush
        on the same dataset yields nothing rather than draining the first
        one's segment twice. Segments left by a crashed flush are picked up
        here, which is the whole of orphan recovery.

        Args:
            ordered: Sort each segment by ``shard`` so the consumer appends
                one parquet file per ``(shard, bucket, origin)`` partition.
                Callers that know the dataset is single-shard pass ``False``
                and skip the sort.
        """
        with self.flush_lock() as acquired:
            if not acquired:
                log.warning(
                    "Another flush is draining this journal – skipping",
                    journal=self.table.name,
                )
                return
            if self._has_rows(self.table.name):
                self._rotate()
            for name in self._segments():
                yield from self._drain(name, ordered)

    def _drain(self, name: str, ordered: bool) -> StatementTables:
        """Stream one segment in whole tables, then drop it.

        Read chunks are gathered into a table *before* it is yielded – which
        costs nothing, the table just references them – and the consumer
        writes each table before asking for the next. So by the time this
        resumes to drop the segment, every row it handed out is durable
        downstream. Yielding chunks the consumer has to buffer would lose the
        tail of a flush whenever the write fails, and a dropped segment is
        gone for good, while a kept one only costs duplicates that
        :meth:`ParquetStore.merge` collapses.
        """
        pending: list[pa.RecordBatch] = []
        rows = 0
        for chunk in self.read_segment(name, ordered):
            pending.append(chunk)
            rows += chunk.num_rows
            if rows >= DRAIN_BATCH_SIZE:
                yield pa.Table.from_batches(pending, schema=SHARDED_SCHEMA)
                pending, rows = [], 0
        if pending:
            yield pa.Table.from_batches(pending, schema=SHARDED_SCHEMA)
        # only after the reader is closed: DROP needs the exclusive lock a
        # still-open read transaction on the same connection would never yield
        self._drop(name)

    # -- reads

    def iterate_entity(self, entity_id: str) -> LakehouseStatements:
        """Iterate the live statements of one entity, across all segments.

        A scan of the journal per call – the heap carries no index by design
        (see :func:`~ftm_lakehouse.model.statement.journal_table`). That is
        the delete path's cost, and it is bounded by how much sits unflushed.
        """
        with self.engine.connect() as conn:
            for name in self._table_names():
                table = self._table(name)
                q = (
                    select(table)
                    .where(table.c.entity_id == entity_id)
                    .where(table.c.deleted_at.is_(None))
                )
                for row in conn.execute(q):
                    yield _row_to_statement(row)

    def count(self) -> int:
        """Count rows for this dataset, across all segments."""
        total = 0
        with self.engine.connect() as conn:
            for name in self._table_names():
                res = conn.exec_driver_sql(f'SELECT count(*) FROM "{name}"').scalar()
                total += res or 0
        return total

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        count = self.count()
        with self.engine.begin() as conn:
            for name in self._segments():
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')
            conn.execute(delete(self.table))
        return count

    def dispose(self) -> None:
        """Dispose the engine and close all pooled connections."""
        self.engine.dispose()


class SqliteJournalStore(SqlJournalStore):
    """Journal on sqlite – the default, and what the test suite runs on."""

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        super().__init__(dataset, uri)
        self._flush_lock = threading.Lock()

    @contextmanager
    def flush_lock(self) -> Generator[bool, None, None]:
        """In-process guard – a sqlite journal has one process by design.

        The store is cached per dataset, so this covers the threads of one
        worker; it dies with the process, so nothing can be stranded.
        """
        acquired = self._flush_lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                self._flush_lock.release()

    def make_engine(self) -> Engine:
        # For in-memory SQLite, use StaticPool to share the same connection
        if self.uri == "sqlite:///:memory:":
            log.warn("Using in-memory journal!")
            return create_engine(
                self.uri,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        return create_engine(self.uri, hide_parameters=True, poolclass=NullPool)

    def connect(self) -> Any:
        return self.engine.connect()

    def insert_batch(self, conn: Any, batch: pa.Table) -> None:
        """Hand the rows to SQLAlchemy's ``executemany``.

        Per-row binding rather than one giant multi-values statement, so the
        batch size is bounded by memory instead of by the driver's parameter
        ceiling. The row shape is SQLAlchemy's cost here, not ours – binding
        dominates, and building the dicts columnwise instead measures the
        same.
        """
        conn.execute(insert(self.table), batch.to_pylist())
        conn.commit()

    def read_segment(self, name: str, ordered: bool) -> RecordBatches:
        """Transpose each cursor chunk columnwise into Arrow.

        Row tuples already arrive in :data:`SHARDED_SCHEMA` column order –
        the table is built from that schema – so the batch is one
        ``zip(*rows)`` away, with no dict per row and no keyed lookup per
        column (~1.6x a ``from_pylist`` of row dicts).
        """
        table = self._table(name)
        q = select(table)
        if ordered:
            q = q.order_by(table.c.shard)
        with self.engine.connect() as conn:
            cursor = conn.execution_options(stream_results=True).execute(q)
            try:
                while rows := cursor.fetchmany(READ_BATCH_SIZE):
                    yield pa.RecordBatch.from_arrays(
                        [
                            pa.array(column, field.type)
                            for column, field in zip(zip(*rows), SHARDED_SCHEMA)
                        ],
                        schema=SHARDED_SCHEMA,
                    )
            finally:
                cursor.close()


ERR_NO_ADBC = ImproperlyConfigured(
    "A postgres journal needs the `postgres` extra installed "
    "(`adbc-driver-postgresql`) for Arrow row IO"
)


class PostgresJournalStore(SqlJournalStore):
    """Journal on postgres – Arrow row IO through ADBC, binary ``COPY``."""

    lock_timeout = ROTATE_LOCK_TIMEOUT

    def make_engine(self) -> Engine:
        # NullPool: connections opened on demand, closed after use.
        # The ``get_journal`` factory is unbounded ``@cache`` (one
        # engine per dataset name) so a default QueuePool of 5+10
        # idle connections per engine multiplies fast under many
        # distinct datasets across multiple workers. NullPool keeps
        # the concurrent connection count bounded by request load
        # rather than by cached engine count – important to stay
        # under postgres ``max_connections``.
        return create_engine(self.uri, hide_parameters=True, poolclass=NullPool)

    @cached_property
    def adbc_uri(self) -> str:
        """The journal uri as a libpq connection string for ADBC."""
        if adbc_pg is None:
            raise ERR_NO_ADBC
        url = make_url(self.uri).set(drivername="postgresql")
        return url.render_as_string(hide_password=False)

    @contextmanager
    def flush_lock(self) -> Generator[bool, None, None]:
        """Session advisory lock, keyed on the journal table.

        Session-scoped rather than transaction-scoped because a flush spans
        many transactions – and because postgres drops it when the
        connection goes, so a crashed flusher releases it for free and the
        next flush recovers its segment.
        """
        key = crc32(self.table.name.encode())
        with self.engine.connect() as conn:
            acquired = bool(
                conn.exec_driver_sql(f"SELECT pg_try_advisory_lock({key})").scalar()
            )
            try:
                yield acquired
            finally:
                if acquired:
                    conn.exec_driver_sql(f"SELECT pg_advisory_unlock({key})")

    def connect(self) -> Connection:
        """Open an ADBC connection for Arrow row IO."""
        if adbc_pg is None:
            raise ERR_NO_ADBC
        return adbc_pg.connect(self.adbc_uri)

    def insert_batch(self, conn: Connection, batch: pa.Table) -> None:
        with conn.cursor() as cur:
            cur.adbc_ingest(self.table.name, batch, mode="append")
        conn.commit()

    def read_segment(self, name: str, ordered: bool) -> RecordBatches:
        order = " ORDER BY shard" if ordered else ""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT {COLUMNS} FROM "{name}"{order}')
                for batch in cur.fetch_record_batch():
                    yield batch.cast(SHARDED_SCHEMA)
        finally:
            conn.close()


def sql_journal(dataset: str, uri: str) -> SqlJournalStore:
    """Pick the dialect implementation once, at construction."""
    if make_url(uri).get_backend_name() in ("postgresql", "postgres"):
        return PostgresJournalStore(dataset, uri)
    return SqliteJournalStore(dataset, uri)
