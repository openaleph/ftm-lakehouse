"""
SQL-based statement journal for buffering writes before flushing to delta lake.

The journal stores statements in a compact format optimized for batch processing,
grouping by origin for efficient partitioned writes to the lake storage.

This is an internal module - client applications should use DatasetEntities.
"""

from datetime import datetime
from typing import Generator

from anystore.types import Uri
from followthemoney import Statement
from ftmq.store.lake import DEFAULT_ORIGIN, get_schema_bucket
from ftmq.types import StatementEntity
from ftmq.util import EntityProxy, ensure_entity
from sqlalchemy import Column, Index, MetaData, String, Table, Text, delete, select
from sqlalchemy.dialects.postgresql import insert as psql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine, Transaction, create_engine
from sqlalchemy.pool import StaticPool

from ftm_lakehouse.conventions import tag
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.exceptions import ImproperlyConfigured

settings = Settings()

WRITE_BATCH_SIZE = 10_000
NULL_BYTE = "\x00"

FlushItem = tuple[str, str, Statement]  # (bucket, origin, statement)
FlushItems = Generator[FlushItem, None, None]


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


def _to_iso(value: datetime | str | None) -> str:
    """Convert a datetime or string to ISO format string."""
    if value is None:
        return datetime.now().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def pack_statement(stmt: Statement) -> str:
    """
    Pack a Statement into a null-byte joined string.

    Format: id, entity_id, canonical_id, prop, schema, value, dataset,
            lang, original_value, external, first_seen, last_seen, origin, prop_type
    """
    row = stmt.to_db_row()
    parts = [
        row["id"],  # required
        row["entity_id"],  # required
        row["canonical_id"],  # required
        row["prop"],  # required
        row["schema"],  # required
        row["value"],  # required
        row["dataset"],  # required
        row.get("lang") or "",
        row.get("original_value") or "",
        "1" if row.get("external") else "0",
        _to_iso(row.get("first_seen")),
        _to_iso(row.get("last_seen")),
        row.get("origin") or DEFAULT_ORIGIN,
        row.get("prop_type") or "",
    ]
    return NULL_BYTE.join(parts)


def unpack_statement(data: str) -> Statement:
    """
    Unpack a null-byte joined string back into a Statement.
    """
    parts = data.split(NULL_BYTE)
    return Statement(
        id=parts[0] or None,
        entity_id=parts[1],  # required
        canonical_id=parts[2] or None,
        prop=parts[3],  # required
        schema=parts[4],  # required
        value=parts[5],  # required
        dataset=parts[6],  # required
        lang=parts[7] or None,
        original_value=parts[8] or None,
        external=parts[9] == "1",
        first_seen=parts[10] or None,
        last_seen=parts[11] or None,
        origin=parts[12] or None,
    )


class JournalWriter:
    """
    Bulk writer for the journal with batched upserts.
    """

    def __init__(self, journal: "Journal", origin: str | None = None) -> None:
        self.journal = journal
        self.origin = origin
        self.batch: list[dict] = []
        self.conn: Connection = journal.engine.connect()
        self.tx: Transaction | None = None

    def _upsert_batch(self) -> None:
        if not self.batch:
            return
        if self.tx is None:
            self.tx = self.conn.begin()

        dialect = self.journal.engine.dialect.name
        table = self.journal.table

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

    def add_statement(self, stmt: Statement) -> None:
        """Add a statement to the journal."""
        if stmt.entity_id is None:
            return

        canonical_id = stmt.canonical_id or stmt.entity_id
        stmt.canonical_id = canonical_id
        origin = self.origin or stmt.origin or DEFAULT_ORIGIN
        # Override the dataset to match the journal's dataset name
        stmt.dataset = self.journal.name

        self.batch.append(
            {
                "id": stmt.id,
                "dataset": self.journal.name,
                "bucket": get_schema_bucket(stmt.schema),
                "origin": origin,
                "canonical_id": canonical_id,
                "data": pack_statement(stmt),
            }
        )

        if len(self.batch) >= WRITE_BATCH_SIZE:
            self._upsert_batch()

    def add_entity(self, entity: EntityProxy) -> None:
        """Add all statements from an entity to the journal."""
        entity = ensure_entity(entity, StatementEntity, self.journal.name)
        origin = self.origin or DEFAULT_ORIGIN
        for stmt in entity.statements:
            stmt.origin = self.origin or stmt.origin or origin
            self.add_statement(stmt)

    def flush(self) -> None:
        """Flush pending statements and commit transaction."""
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

    def __enter__(self) -> "JournalWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if exc_type is not None:
            self.rollback()
        else:
            self.flush()
        self.close()


class Journal(LakeMixin):
    """
    SQL-based statement journal for buffering writes before flushing to delta lake.

    Stores statements in a compact format with columns optimized for batch
    processing: dataset, bucket, origin, canonical_id, and packed statement data.

    Args:
        name: Dataset name
        uri: Lake storage URI
        journal_uri: SQLAlchemy database URI for the journal (default from settings)
    """

    def __init__(
        self, name: str, uri: Uri, journal_uri: str | None = settings.journal_uri
    ) -> None:
        super().__init__(name, uri)
        db_uri = journal_uri or settings.journal_uri
        # For in-memory SQLite, use StaticPool to share the same connection
        # across all threads, otherwise each connection gets a new database
        if db_uri == "sqlite:///:memory:":
            if not settings.debug:
                raise ImproperlyConfigured(
                    f"Statement journal must be persistent, not `{db_uri}`"
                )
            self.log.warn("Using in-memory statement journal")
            self.engine: Engine = create_engine(
                db_uri,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            self.engine = create_engine(db_uri)
        self.metadata = MetaData()
        self.table = make_journal_table(self.metadata, f"journal_{name}")
        self.metadata.create_all(self.engine, tables=[self.table], checkfirst=True)

    def writer(self, origin: str | None = None) -> JournalWriter:
        """Get a bulk writer for adding statements."""
        return JournalWriter(self, origin=origin)

    def put(self, stmt: Statement) -> None:
        """Write a single statement to the journal."""
        with self.writer() as w:
            w.add_statement(stmt)

    def add_entity(self, entity: EntityProxy, origin: str | None = None) -> None:
        """Write all statements from an entity to the journal."""
        entity = ensure_entity(entity, StatementEntity, self.name)
        with self.writer() as w:
            for stmt in entity.statements:
                stmt.origin = origin or stmt.origin or DEFAULT_ORIGIN
                w.add_statement(stmt)

    def flush(self) -> FlushItems:
        """
        Iterate over all statements in the journal for this dataset.

        Streams statements ordered by (bucket, origin, canonical_id), yielding
        (bucket, origin, statement) tuples. The consumer is responsible for
        tracking bucket/origin changes and flushing writers accordingly.

        Deletes all statements for this dataset at the end within the same
        transaction.

        This is a destructive read operation. The journal transaction remains
        open until all items are consumed. If the consumer raises an exception,
        the transaction is rolled back and statements are preserved.

        Yields:
            Tuples of (bucket, origin, Statement)
        """
        with self.get_lock(tag.JOURNAL_FLUSHING):
            q = (
                select(self.table)
                .where(self.table.c.dataset == self.name)
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
                            stmt = unpack_statement(row.data)
                            yield row.bucket, row.origin, stmt

                    # Delete all statements for this dataset
                    conn.execute(
                        delete(self.table).where(self.table.c.dataset == self.name)
                    )
                    tx.commit()
                except BaseException:
                    tx.rollback()
                    raise
