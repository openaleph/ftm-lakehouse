"""JournalStore - SQL statement buffer for write-ahead logging."""

from anystore.logging import get_logger
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    MetaData,
    String,
    Table,
    Text,
    delete,
    select,
)
from sqlalchemy.dialects.postgresql import insert as psql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine, Transaction, create_engine
from sqlalchemy.pool import StaticPool

from ftm_lakehouse.storage.journal.base import (
    BaseJournalStore,
    BaseJournalWriter,
    JournalRows,
)

log = get_logger(__name__)


def make_journal_table(metadata: MetaData, dataset: str) -> Table:
    """Create the journal table schema."""
    return Table(
        f"journal_{dataset}",
        metadata,
        Column("id", String(255), primary_key=True),
        Column("bucket", String(50), nullable=False),
        Column("origin", String(255), nullable=False),
        Column("canonical_id", String(255), nullable=False),
        Column("data", Text, nullable=False),
        Column("deleted_at", DateTime(timezone=True), nullable=True),
        Index(f"ix_{dataset}_sort", "bucket", "origin", "canonical_id"),
    )


class SqlJournalWriter(BaseJournalWriter["SqlJournalStore"]):
    """SQL-backed bulk writer with batched upserts."""

    def __init__(self, store: "SqlJournalStore", origin: str | None = None) -> None:
        super().__init__(store, origin)
        self.conn: Connection = store.engine.connect()
        self.tx: Transaction | None = None

    def _upsert_batch(self) -> None:
        if not self.batch:
            return
        if self.tx is None:
            self.tx = self.conn.begin()

        dialect = self.store.engine.dialect.name
        table = self.store.table

        if dialect == "sqlite":
            sqlite_istmt = sqlite_insert(table).values(self.batch)
            sqlite_stmt = sqlite_istmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "bucket": sqlite_istmt.excluded.bucket,
                    "origin": sqlite_istmt.excluded.origin,
                    "canonical_id": sqlite_istmt.excluded.canonical_id,
                    "data": sqlite_istmt.excluded.data,
                    "deleted_at": sqlite_istmt.excluded.deleted_at,
                },
            )
            self.conn.execute(sqlite_stmt)
        elif dialect in ("postgresql", "postgres"):
            psql_istmt = psql_insert(table).values(self.batch)
            psql_stmt = psql_istmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "bucket": psql_istmt.excluded.bucket,
                    "origin": psql_istmt.excluded.origin,
                    "canonical_id": psql_istmt.excluded.canonical_id,
                    "data": psql_istmt.excluded.data,
                    "deleted_at": psql_istmt.excluded.deleted_at,
                },
            )
            self.conn.execute(psql_stmt)
        else:
            raise NotImplementedError(f"Upsert not implemented for dialect {dialect}")

        self.batch = []

    def flush(self) -> None:
        """Flush pending rows and commit transaction."""
        self._upsert_batch()
        if self.tx is not None:
            self.tx.commit()
            self.tx = None

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self.tx is not None:
            self.tx.rollback()
            self.tx = None

    def close(self) -> None:
        """Close the connection."""
        self.conn.close()


class SqlJournalStore(BaseJournalStore[SqlJournalWriter]):
    """
    SQL-based journal for buffering writes.

    Stores rows in a SQL table with upsert semantics, supporting
    batch writes and transactional flush operations.
    """

    _writer_cls = SqlJournalWriter

    def __init__(self, dataset: str, uri: str | None = None) -> None:
        super().__init__(dataset, uri)

        # For in-memory SQLite, use StaticPool to share the same connection
        if self.uri == "sqlite:///:memory:":
            log.warn("Using in-memory journal!")
            self.engine: Engine = create_engine(
                self.uri,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            self.engine = create_engine(self.uri)

        self.metadata = MetaData()
        self.table = make_journal_table(self.metadata, dataset)
        self.metadata.create_all(self.engine, tables=[self.table], checkfirst=True)

    def iterate(self) -> JournalRows:
        """Iterate all rows ordered for batch processing."""
        q = select(self.table).order_by(
            self.table.c.bucket,
            self.table.c.origin,
            self.table.c.canonical_id,
        )

        with self.engine.connect() as conn:
            cursor = conn.execution_options(stream_results=True).execute(q)
            while rows := cursor.fetchmany(10_000):
                for row in rows:
                    yield (
                        row.id,
                        row.bucket,
                        row.origin,
                        row.canonical_id,
                        row.data,
                        row.deleted_at,
                    )

    def flush(self) -> JournalRows:
        """Iterate and delete all rows atomically."""
        q = select(self.table).order_by(
            self.table.c.bucket,
            self.table.c.origin,
            self.table.c.canonical_id,
        )

        with self.engine.connect() as conn:
            tx = conn.begin()
            try:
                cursor = conn.execution_options(stream_results=True).execute(q)

                while rows := cursor.fetchmany(10_000):
                    for row in rows:
                        yield row.id, row.bucket, row.origin, row.canonical_id, row.data, row.deleted_at

                # Delete all rows for this dataset
                conn.execute(delete(self.table))
                tx.commit()
            except BaseException:
                tx.rollback()
                raise

    def count(self) -> int:
        """Count rows for this dataset."""
        from sqlalchemy import func

        q = select(func.count()).select_from(self.table)
        with self.engine.connect() as conn:
            result = conn.execute(q).scalar()
            return result or 0

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        count = self.count()
        with self.engine.connect() as conn:
            conn.execute(delete(self.table))
            conn.commit()
        return count

    def dispose(self) -> None:
        """Dispose the engine and close all pooled connections."""
        self.engine.dispose()

    def __del__(self) -> None:
        """Clean up engine on garbage collection."""
        try:
            self.engine.dispose()
        except Exception:
            pass
