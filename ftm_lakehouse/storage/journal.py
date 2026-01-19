"""JournalStore - SQL statement buffer for write-ahead logging."""

from typing import Generator, Self, TypeAlias

from anystore.logging import get_logger
from followthemoney import EntityProxy, Statement, StatementEntity
from ftmq.store.lake import DEFAULT_ORIGIN, get_schema_bucket
from ftmq.util import ensure_entity
from sqlalchemy import Column, Index, MetaData, String, Table, Text, delete, select
from sqlalchemy.dialects.postgresql import insert as psql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine, Transaction, create_engine
from sqlalchemy.pool import StaticPool

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import pack_statement

settings = Settings()
log = get_logger(__name__)

WRITE_BATCH_SIZE = 10_000

JournalRow = tuple[str, str, str, str, str]  # (id, bucket, origin, canonical_id, data)
JournalRows: TypeAlias = Generator[JournalRow, None, None]


def make_journal_table(metadata: MetaData, name: str = "journal") -> Table:
    """Create the journal table schema."""
    return Table(
        name,
        metadata,
        Column("id", String(255), primary_key=True),
        Column("dataset", String(255), nullable=False),
        Column("bucket", String(50), nullable=False),
        Column("origin", String(255), nullable=False),
        Column("canonical_id", String(255), nullable=False),
        Column("data", Text, nullable=False),
        Index(f"ix_{name}_sort", "dataset", "bucket", "origin", "canonical_id"),
    )


class JournalWriter:
    """
    Bulk writer for the journal with batched upserts.

    Not intended for direct use - use JournalStore.writer() instead.
    """

    def __init__(self, store: "JournalStore", origin: str | None = None) -> None:
        self.store = store
        self.dataset = store.dataset
        self.origin = origin or DEFAULT_ORIGIN
        self.batch: list[dict] = []
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
                    "dataset": sqlite_istmt.excluded.dataset,
                    "bucket": sqlite_istmt.excluded.bucket,
                    "origin": sqlite_istmt.excluded.origin,
                    "canonical_id": sqlite_istmt.excluded.canonical_id,
                    "data": sqlite_istmt.excluded.data,
                },
            )
            self.conn.execute(sqlite_stmt)
        elif dialect in ("postgresql", "postgres"):
            psql_istmt = psql_insert(table).values(self.batch)
            psql_stmt = psql_istmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "dataset": psql_istmt.excluded.dataset,
                    "bucket": psql_istmt.excluded.bucket,
                    "origin": psql_istmt.excluded.origin,
                    "canonical_id": psql_istmt.excluded.canonical_id,
                    "data": psql_istmt.excluded.data,
                },
            )
            self.conn.execute(psql_stmt)
        else:
            raise NotImplementedError(f"Upsert not implemented for dialect {dialect}")

        self.batch = []

    def add(
        self,
        row_id: str,
        dataset: str,
        bucket: str,
        origin: str,
        canonical_id: str,
        data: str,
    ) -> None:
        """Add a raw row to the journal batch."""
        self.batch.append(
            {
                "id": row_id,
                "dataset": dataset,
                "bucket": bucket,
                "origin": origin,
                "canonical_id": canonical_id,
                "data": data,
            }
        )

        if len(self.batch) >= WRITE_BATCH_SIZE:
            self._upsert_batch()

    def add_statement(self, stmt: Statement) -> None:
        """Add a statement to the journal."""
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

        self.add(
            row_id=stmt.id,
            dataset=self.dataset,
            bucket=get_schema_bucket(stmt.schema),
            origin=origin,
            canonical_id=canonical_id,
            data=pack_statement(stmt),
        )

    def add_entity(self, entity: EntityProxy) -> None:
        """Add all statements from an entity to the journal."""
        entity = ensure_entity(entity, StatementEntity, self.dataset)
        for stmt in entity.statements:
            self.add_statement(stmt)

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

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if exc_type is not None:
            self.rollback()
        else:
            self.flush()
        self.close()


class JournalStore:
    """
    SQL-based journal for buffering writes.

    Stores rows in a SQL table with upsert semantics, supporting
    batch writes and transactional flush operations.

    The journal is designed as a write-ahead log - data is written
    here first, then flushed to permanent parquet storage.

    Args:
        dataset: Dataset name (used for table name and filtering)
        uri: SQLAlchemy database URI
    """

    def __init__(
        self,
        dataset: str,
        uri: str | None = None,
    ) -> None:
        self.dataset = dataset
        db_uri = uri or settings.journal_uri

        # For in-memory SQLite, use StaticPool to share the same connection
        if db_uri == "sqlite:///:memory:":
            log.warn("Using in-memory journal!")
            self.engine: Engine = create_engine(
                db_uri,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            self.engine = create_engine(db_uri)

        self.metadata = MetaData()
        self.table = make_journal_table(self.metadata, f"journal_{dataset}")
        self.metadata.create_all(self.engine, tables=[self.table], checkfirst=True)

    def writer(self, origin: str | None = None) -> JournalWriter:
        """Get a bulk writer for adding rows."""
        return JournalWriter(self, origin=origin)

    def iterate(self) -> JournalRows:
        """
        Iterate all rows for this dataset, ordered for batch processing.

        Rows are ordered by (bucket, origin, canonical_id) for efficient
        partitioned writes to downstream storage.

        Yields:
            Tuples of (id, bucket, origin, canonical_id, data)
        """
        q = (
            select(self.table)
            .where(self.table.c.dataset == self.dataset)
            .order_by(
                self.table.c.bucket,
                self.table.c.origin,
                self.table.c.canonical_id,
            )
        )

        with self.engine.connect() as conn:
            cursor = conn.execution_options(stream_results=True).execute(q)
            while rows := cursor.fetchmany(10_000):
                for row in rows:
                    yield row.id, row.bucket, row.origin, row.canonical_id, row.data

    def flush(self) -> JournalRows:
        """
        Iterate and delete all rows for this dataset atomically.

        This is a destructive read - rows are deleted after being yielded.
        If the consumer raises an exception, the transaction is rolled back.

        Yields:
            Tuples of (id, bucket, origin, canonical_id, data)
        """
        q = (
            select(self.table)
            .where(self.table.c.dataset == self.dataset)
            .order_by(
                self.table.c.bucket,
                self.table.c.origin,
                self.table.c.canonical_id,
            )
        )

        with self.engine.connect() as conn:
            tx = conn.begin()
            try:
                cursor = conn.execution_options(stream_results=True).execute(q)

                while rows := cursor.fetchmany(10_000):
                    for row in rows:
                        yield row.id, row.bucket, row.origin, row.canonical_id, row.data

                # Delete all rows for this dataset
                conn.execute(
                    delete(self.table).where(self.table.c.dataset == self.dataset)
                )
                tx.commit()
            except BaseException:
                tx.rollback()
                raise

    def count(self) -> int:
        """Count rows for this dataset."""
        from sqlalchemy import func

        q = (
            select(func.count())
            .select_from(self.table)
            .where(self.table.c.dataset == self.dataset)
        )
        with self.engine.connect() as conn:
            result = conn.execute(q).scalar()
            return result or 0

    def clear(self) -> int:
        """Delete all rows for this dataset. Returns count of deleted rows."""
        count = self.count()
        with self.engine.connect() as conn:
            conn.execute(delete(self.table).where(self.table.c.dataset == self.dataset))
            conn.commit()
        return count
