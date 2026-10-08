"""SqlJournalStore – SQL statement buffer for write-ahead logging."""

from __future__ import annotations

import threading
from binascii import crc32
from contextlib import contextmanager
from functools import cached_property, partial
from typing import Any, Generator
from uuid import uuid4

import pyarrow as pa
from anystore.logging import get_logger
from ftmq.util import datetime_iso
from rigour.time import utc_now
from sqlalchemy import MetaData, Table, delete, insert, inspect, select
from sqlalchemy.engine import Engine, create_engine, make_url
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.pool import NullPool, Pool, QueuePool, StaticPool
from sqlalchemy.schema import CreateTable

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import ImproperlyConfigured
from ftm_lakehouse.model.statement import (
    JOURNAL_SCHEMA,
    LakehouseStatement,
    LakehouseStatements,
    journal_table,
)
from ftm_lakehouse.storage.journal.base import (
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

settings = Settings()
log = get_logger(__name__)

READ_BATCH_SIZE = 10_000
"""Rows per cursor fetch when draining a segment through SQLAlchemy."""

SEGMENT_INFIX = "-seg-"
"""Separates a journal table from its rotated segments."""

ROTATE_LOCK_TIMEOUT = "5s"

COLUMNS = ", ".join(f'"{name}"' for name in JOURNAL_SCHEMA.names)


def _row_to_statement(row: Any) -> LakehouseStatement:
    """Build a statement from a journal row."""
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
        role=row.role,
        deleted_at=row.deleted_at,
    )


class SqlJournalWriter(BaseJournalWriter["SqlJournalStore"]):
    """SQL bulk writer – borrows one store connection for its lifetime."""

    _conn: Any = None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self.store.acquire()
        return self._conn

    def _insert(self, batch: pa.Table) -> None:
        try:
            self.store.insert_batch(self.conn, batch)
        except Exception:
            # releasing rolls back postgres's aborted transaction, so a caller
            # that catches the error and keeps writing gets a usable connection
            self.close()
            raise

    def close(self) -> None:
        """Hand the connection back to the store."""
        if self._conn is not None:
            self.store.release(self._conn)
            self._conn = None


class SqlJournalStore(BaseJournalStore[SqlJournalWriter]):
    """SQL journal – an append-only heap per dataset in `JOURNAL_SCHEMA`.

    A flush renames the table to a timestamped segment and creates a fresh one
    in the same DDL transaction, streams the segment out as Arrow and drops it
    – cleanup is a catalog operation, never a ``DELETE``. Dialects:
    `SqliteJournalStore` / `PostgresJournalStore`, picked by `sql_journal`.
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
        # no idle connections: `get_journal` caches a store per dataset forever
        return create_engine(self.uri, hide_parameters=True, poolclass=NullPool)

    def connect(self) -> Any:
        """Open a connection for a writer's inserts."""
        raise NotImplementedError

    def acquire(self) -> Any:
        """Take a connection for a writer's inserts.

        A plain [`connect`][SqlJournalStore.connect]; `PostgresJournalStore`
        borrows from an ADBC pool instead.
        """
        return self.connect()

    def release(self, conn: Any) -> None:
        """Hand a writer's connection back by closing it.

        On postgres that checks it into the pool, rolled back, so the next
        writer never inherits an aborted transaction.
        """
        conn.close()

    def insert_batch(self, conn: Any, batch: pa.Table) -> None:
        """Append one packed batch to the live table."""
        raise NotImplementedError

    def read_segment(self, name: str) -> RecordBatches:
        """Stream a segment's rows."""
        raise NotImplementedError

    @contextmanager
    def flush_lock(self) -> Generator[bool, None, None]:
        """Hold this dataset's flush window; yields ``False`` if another flush has it.

        Without it a second flush would drain the first one's segment. The lock
        must release itself when its holder dies, or a crash strands a segment.
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
        """Rotated segments, oldest first – orphans of a crashed flush included."""
        names = inspect(self.engine).get_table_names()
        return sorted(n for n in names if n.startswith(self._prefix))

    def _table_names(self) -> list[str]:
        """The live table plus every un-dropped segment."""
        return [self.table.name, *self._segments()]

    def _table(self, name: str) -> Table:
        return journal_table(MetaData(), name)

    def _rotate(self) -> None:
        """Rename the journal to a segment and recreate it in one DDL transaction.

        The rename's exclusive lock waits out in-flight inserts, so no row lands
        in the segment afterwards; blocked writers continue into the fresh table.
        `lock_timeout` bounds the wait – every new insert queues behind it – so a
        flush blocked by a long transaction fails, and the next flush retries.
        """
        name = self._segment_name()
        with self.engine.begin() as conn:
            self._set_lock_timeout(conn)
            conn.exec_driver_sql(f'ALTER TABLE "{self.table.name}" RENAME TO "{name}"')
            conn.execute(CreateTable(self.table))

    def _drop(self, name: str) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')

    def _has_rows(self, name: str) -> bool:
        with self.engine.connect() as conn:
            res = conn.exec_driver_sql(f'SELECT 1 FROM "{name}" LIMIT 1')
            return res.first() is not None

    # -- flush

    def flush_batches(self) -> StatementTables:
        """Rotate the journal, then stream each segment as Arrow tables and drop it.

        Held under [`flush_lock`][SqlJournalStore.flush_lock] – a concurrent
        flush yields nothing. Segments left by a crashed flush are drained too,
        and a consumer that fails keeps the undrained rows for the next flush.
        Rows stream unordered, so a table spans shards and
        [`append`][ftm_lakehouse.storage.parquet.ParquetStore.append] writes one
        file per partition it touches.
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
                yield from self._drain(name)

    def _drain(self, name: str) -> StatementTables:
        """Stream one segment in whole tables, then drop it.

        The consumer writes each table before asking for the next, so the drop
        only follows durable writes; a failure keeps the segment.
        """
        pending: list[pa.RecordBatch] = []
        rows = 0
        for chunk in self.read_segment(name):
            pending.append(chunk)
            rows += chunk.num_rows
            if rows >= settings.journal_drain_rows:
                yield pa.Table.from_batches(pending, schema=JOURNAL_SCHEMA)
                pending, rows = [], 0
        if pending:
            yield pa.Table.from_batches(pending, schema=JOURNAL_SCHEMA)
        # only after the reader is closed – its read transaction blocks the DROP
        self._drop(name)

    # -- reads

    def iterate_entity(self, entity_id: str) -> LakehouseStatements:
        """Iterate the live statements of one entity, across all segments.

        A full scan per call – the journal has no index (see
        [`journal_table`][ftm_lakehouse.model.statement.journal_table]).
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
        """In-process lock – a sqlite journal has one process by design."""
        acquired = self._flush_lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                self._flush_lock.release()

    def make_engine(self) -> Engine:
        # in-memory: one shared connection, or each would see its own database
        if self.uri == "sqlite:///:memory:":
            log.warn("Using in-memory journal!")
            return create_engine(
                self.uri,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        return super().make_engine()

    def connect(self) -> Any:
        return self.engine.connect()

    def insert_batch(self, conn: Any, batch: pa.Table) -> None:
        """Hand the rows to SQLAlchemy's ``executemany`` – per-row binding, so no
        driver parameter limit caps the batch size."""
        conn.execute(insert(self.table), batch.to_pylist())
        conn.commit()

    def read_segment(self, name: str) -> RecordBatches:
        """Transpose each cursor chunk columnwise into Arrow – rows arrive in
        `JOURNAL_SCHEMA` column order."""
        q = select(self._table(name))
        with self.engine.connect() as conn:
            cursor = conn.execution_options(stream_results=True).execute(q)
            try:
                while rows := cursor.fetchmany(READ_BATCH_SIZE):
                    yield pa.RecordBatch.from_arrays(
                        [
                            pa.array(column, field.type)
                            for column, field in zip(zip(*rows), JOURNAL_SCHEMA)
                        ],
                        schema=JOURNAL_SCHEMA,
                    )
            finally:
                cursor.close()


ERR_NO_ADBC = ImproperlyConfigured(
    "A postgres journal needs the `postgres` extra installed "
    "(`adbc-driver-postgresql`) for Arrow row IO"
)


def _ping_on_checkout(conn: Any, record: Any, proxy: Any) -> None:
    """Ping a pooled ADBC connection on checkout.

    A connection the server dropped while idle raises `DisconnectionError`,
    which makes the pool retire it and dial a fresh one for the writer.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception as exc:
        raise DisconnectionError(f"Journal connection is dead: {exc}") from exc


def _adbc_connect(uri: str) -> Any:
    """Dial one ADBC connection – the pools' creator, bound to a uri."""
    return adbc_pg.connect(uri)


_POOLS: dict[str, Pool] = {}
"""ADBC pools by journal uri – see `PostgresJournalStore.pool`."""

_POOLS_LOCK = threading.Lock()
"""Guards `_POOLS`: one store is shared across a worker's threads."""


class PostgresJournalStore(SqlJournalStore):
    """Journal on postgres – Arrow row IO through ADBC, binary ``COPY``."""

    lock_timeout = ROTATE_LOCK_TIMEOUT

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        if adbc_pg is None:  # before the engine touches the server
            raise ERR_NO_ADBC
        super().__init__(dataset, uri)

    @cached_property
    def adbc_uri(self) -> str:
        """The journal uri as a libpq connection string for ADBC."""
        url = make_url(self.uri).set(drivername="postgresql")
        return url.render_as_string(hide_password=False)

    @contextmanager
    def flush_lock(self) -> Generator[bool, None, None]:
        """Session advisory lock keyed on the journal table.

        Session-scoped since a flush spans many transactions; postgres drops it
        with the connection, so a crashed flusher releases it.
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
        return _adbc_connect(self.adbc_uri)

    def pool(self) -> Pool:
        """The writers' ADBC connection pool (SQLAlchemy's), shared per uri.

        Keyed on the uri, not the store: ``get_journal`` caches a store per
        dataset forever, so per-store pools would size idle connections by the
        dataset count. ``settings.journal_pool_size`` bounds idle connections per
        process (``0`` pools nothing); writers beyond it open their own rather
        than queueing. Built under a lock – ``cached_property`` has none since
        python 3.12.
        """
        uri = self.adbc_uri
        with _POOLS_LOCK:
            pool = _POOLS.get(uri)
            if pool is None:
                if settings.journal_pool_size < 1:
                    pool = NullPool(partial(_adbc_connect, uri))
                else:
                    pool = QueuePool(
                        partial(_adbc_connect, uri),
                        pool_size=settings.journal_pool_size,
                        max_overflow=-1,
                        events=[(_ping_on_checkout, "checkout")],
                    )
                _POOLS[uri] = pool
            return pool

    def acquire(self) -> Any:
        return self.pool().connect()

    def dispose(self) -> None:
        """Close the pooled connections along with the engine's.

        Drops the uri's pool, shared with the other datasets on that journal –
        they rebuild it on their next writer.
        """
        with _POOLS_LOCK:
            pool = _POOLS.pop(self.adbc_uri, None)
        if pool is not None:
            pool.dispose()
        super().dispose()

    def insert_batch(self, conn: Any, batch: pa.Table) -> None:
        with conn.cursor() as cur:
            cur.adbc_ingest(self.table.name, batch, mode="append")
        conn.commit()

    def read_segment(self, name: str) -> RecordBatches:
        """Stream a segment's rows through a pooled connection.

        Released at the end – check-in rolls back, so the ``DROP`` that follows
        is not blocked by an open read transaction.
        """
        conn = self.acquire()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT {COLUMNS} FROM "{name}"')
                for batch in cur.fetch_record_batch():
                    yield batch.cast(JOURNAL_SCHEMA)
        finally:
            self.release(conn)


def sql_journal(dataset: str, uri: str) -> SqlJournalStore:
    """Pick the dialect implementation once, at construction."""
    if make_url(uri).get_backend_name() in ("postgresql", "postgres"):
        return PostgresJournalStore(dataset, uri)
    return SqliteJournalStore(dataset, uri)
