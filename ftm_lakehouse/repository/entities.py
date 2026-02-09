"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, Iterable

import duckdb
import orjson
import pyarrow as pa
from anystore.io import smart_write_json
from anystore.store import get_store
from anystore.types import SDict, Uri
from anystore.util import Took
from deltalake import write_deltalake
from deltalake.exceptions import TableNotFoundError
from followthemoney import EntityProxy, Statement, StatementEntity
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import (
    TARGET_SIZE,
    WRITER,
)
from ftmq.store.lake import pack_statement as lake_pack_statement
from ftmq.store.lake import (
    storage_options,
)
from ftmq.types import StatementEntities, ValueEntities
from sqlalchemy import select

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import unpack_statement, unpack_tombstone_row
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.repository.diff import ParquetDiffMixin
from ftm_lakehouse.storage import JournalStore, ParquetStore
from ftm_lakehouse.storage.journal import JournalWriter
from ftm_lakehouse.storage.parquet import PARTITIONS, STATEMENT_SCHEMA

settings = Settings()


class EntityRepository(ParquetDiffMixin, BaseRepository):
    """
    Repository for entity/statement operations.

    Combines JournalStore (write-ahead buffer) and ParquetStore (Delta Lake)
    to provide buffered statement storage with efficient querying.

    Writes go to the journal first, then are flushed to the parquet store.
    Reads query the parquet store (optionally flushing first).

    Example:
        ```python
        repo = EntityRepository(uri="s3://bucket/dataset", dataset="my_data")

        # Write entities
        with repo.bulk(origin="import") as writer:
            writer.add_entity(entity)

        # Flush to parquet
        repo.flush()

        # Query entities
        for entity in repo.query(origin="import"):
            process(entity)
        ```
    """

    def __init__(
        self,
        dataset: str,
        uri: Uri,
        journal_uri: str | None = None,
    ) -> None:
        super().__init__(dataset, uri)
        self._journal = JournalStore(dataset, journal_uri or settings.journal_uri)
        self._statements = ParquetStore(uri, dataset)
        self._store = get_store(uri)

    @contextmanager
    def bulk(self, origin: str | None = None) -> Generator[JournalWriter, None, None]:
        """
        Get a bulk writer for adding entities/statements.

        Usage:
            with repo.bulk(origin="import") as writer:
                writer.add_entity(entity)
        """
        writer = self._journal.writer(origin)
        try:
            yield writer
        except BaseException:
            writer.rollback()
            raise
        else:
            writer.flush()
        finally:
            writer.close()
            self._tags.set(tag.JOURNAL_UPDATED)

    def add(self, entity: EntityProxy, origin: str | None = None) -> None:
        """Add a single entity to the journal."""
        with self.bulk(origin) as writer:
            writer.add_entity(entity)

    def add_many(
        self, entities: Iterable[EntityProxy], origin: str | None = None
    ) -> None:
        """Add an entity iterator to the journal."""
        with self.bulk(origin) as writer:
            for entity in entities:
                writer.add_entity(entity)

    def flush(self) -> int:
        """
        Flush statements from journal to parquet store.

        Statements are streamed ordered by (bucket, origin, canonical_id).
        Batches are written directly via write_deltalake, with schema_mode="merge"
        to handle tombstone rows that include a deleted_at column.

        Returns:
            Number of statements flushed
        """
        if self._journal.count() == 0:
            self.log.debug("Journal is empty", journal=self._journal.uri)
            # set tags for the initial run
            if not self._tags.exists(tag.JOURNAL_FLUSHED):
                self._tags.set(tag.JOURNAL_FLUSHED)
            if not self._tags.exists(tag.STATEMENTS_UPDATED):
                self._tags.set(tag.STATEMENTS_UPDATED)
            return 0

        with self._tags.touch(tag.JOURNAL_FLUSHED), Took() as t:
            self.log.info("Flushing journal ...", journal=self._journal.uri)

            total_count = 0
            current_bucket: str | None = None
            current_origin: str | None = None
            current_batch: list[dict] = []

            for _, bucket, origin, _, data, deleted_at in self._journal.flush():
                # Write batch when partition changes
                if bucket != current_bucket or origin != current_origin:
                    if current_batch:
                        self._write_flush_batch(current_batch)
                    current_batch = []
                    current_bucket = bucket
                    current_origin = origin

                if deleted_at is not None:
                    row = unpack_tombstone_row(data)
                    row["deleted_at"] = deleted_at
                else:
                    stmt = unpack_statement(data)
                    row = lake_pack_statement(stmt)
                    row["deleted_at"] = None
                current_batch.append(row)
                total_count += 1

            # Write final batch
            if current_batch:
                self._write_flush_batch(current_batch)

            self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Flushed statements from journal to lake",
                count=total_count,
                took=t.took,
                journal=self._journal.uri,
            )

            return total_count

    def _write_flush_batch(self, batch: list[dict]) -> None:
        """Write a partition batch directly via write_deltalake."""
        table = pa.Table.from_pylist(batch, schema=STATEMENT_SCHEMA)
        write_deltalake(
            str(self._statements.uri),
            table,
            partition_by=PARTITIONS,
            mode="append",
            schema_mode="merge",
            writer_properties=WRITER,
            target_file_size=TARGET_SIZE,
            storage_options=storage_options(),
            configuration={"delta.enableChangeDataFeed": "true"},
        )

    def query(
        self,
        entity_ids: Iterable[str] | None = None,
        flush_first: bool = True,
        **filters,
    ) -> StatementEntities:
        """
        Query entities from the parquet store.

        Additional filter kwargs are passed to ftmq Query.

        Args:
            entity_ids: Filter by entity IDs
            flush_first: Flush journal before querying (default True)

        Yields:
            StatementEntity objects matching the query
        """
        if flush_first:
            self.flush()

        if entity_ids:
            filters["entity_id__in"] = list(entity_ids)
        q = Query().where(**filters)

        yield from self._statements.query(q)

    def get(
        self,
        entity_id: str,
        origin: str | None = None,
        flush_first: bool = True,
    ) -> StatementEntity | None:
        """Get a single entity by ID."""
        for entity in self.query([entity_id], flush_first, origin=origin):
            return entity
        return None

    def stream(self) -> ValueEntities:
        """
        Stream entities from the exported JSON file.

        This reads from the pre-exported entities.ftm.json file,
        not directly from the parquet store.
        """
        uri = self._store.to_uri(path.ENTITIES_JSON)
        yield from smart_read_proxies(uri)

    def delete_entity(self, entity_id: str) -> int:
        """Delete all statements for an entity via journal tombstones.

        Reads statements from both parquet and journal, then UPSERTs
        tombstone rows (with deleted_at set) into the journal.

        Args:
            entity_id: The entity ID to delete

        Returns:
            Number of tombstone statements written
        """
        now = datetime.now(timezone.utc)
        stmts = self._collect_entity_statements(entity_id)
        if not stmts:
            return 0
        with self._journal.writer() as w:
            for stmt in stmts:
                w.add_statement(stmt, deleted_at=now)
        self._tags.set(tag.JOURNAL_UPDATED)
        return len(stmts)

    def delete_statement(self, stmt: Statement) -> None:
        """Delete a single statement via journal tombstone.

        Args:
            stmt: The Statement to delete
        """
        now = datetime.now(timezone.utc)
        with self._journal.writer() as w:
            w.add_statement(stmt, deleted_at=now)
        self._tags.set(tag.JOURNAL_UPDATED)

    def _collect_entity_statements(self, entity_id: str) -> list[Statement]:
        """Read all statements for an entity from parquet + journal."""
        stmts_by_id: dict[str, Statement] = {}

        # Read from parquet
        try:
            dt = self._statements._store.deltatable
            rel = duckdb.arrow(dt.to_pyarrow_dataset())
            cols = {f.name for f in dt.schema().to_arrow()}
            where = f"entity_id = '{entity_id}'"
            if "deleted_at" in cols:
                where += " AND deleted_at IS NULL"
            result = rel.query(
                "arrow",
                f"SELECT * FROM arrow WHERE {where}",
            )
            for row in result.fetchall():
                row_dict = dict(zip(result.columns, row))
                stmt = Statement.from_dict(row_dict)
                if stmt.id:
                    stmts_by_id[stmt.id] = stmt
        except TableNotFoundError:
            pass

        # Read from journal (may override parquet entries)

        q = (
            select(self._journal.table)
            .where(self._journal.table.c.dataset == self._journal.dataset)
            .where(self._journal.table.c.canonical_id == entity_id)
            .where(self._journal.table.c.deleted_at.is_(None))
        )
        with self._journal.engine.connect() as conn:
            for row in conn.execute(q):
                stmt = unpack_statement(row.data)
                if stmt.id:
                    stmts_by_id[stmt.id] = stmt

        return list(stmts_by_id.values())

    def make_statistics(self) -> DatasetStats:
        """Compute statistics from the parquet store."""
        return self._statements.stats()

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_ENTITIES

    def _filter_changes(
        self,
        changes: Generator[tuple[datetime, str, dict], None, None],
    ) -> set[str]:
        """Track all entity IDs that have any statement change."""
        changed_entity_ids: set[str] = set()
        for _, change_type, row in changes:
            if change_type in ("insert", "update_postimage"):
                changed_entity_ids.add(row["entity_id"])
        return changed_entity_ids

    def _write_diff(self, entity_ids: set[str], v: int, ts: datetime, **kwargs) -> str:
        """Write entities as line-based JSON with operation envelopes."""
        key = path.entities_diff(v, ts)
        with self._store.open(key, "wb") as o:
            smart_write_json(o, self._get_delta_entities(entity_ids))
        return self._store.to_uri(key)

    def _get_delta_entities(self, entity_ids: set[str]) -> Generator[SDict, None, None]:
        seen_ids: set[str] = set()
        for entity in self.query(entity_ids=entity_ids, flush_first=False):
            seen_ids.add(entity.id)
            yield self._make_envelope(entity.to_dict())
        # Entities in changed set but not in deduped output were deleted
        for entity_id in entity_ids - seen_ids:
            yield self._make_envelope({"id": entity_id}, op="DEL")

    def _make_envelope(self, data: SDict, op: str = "ADD") -> SDict:
        return {"op": op, "entity": data}

    def _write_initial_diff(self, version: int, ts: datetime, **kwargs) -> None:
        """Copy over exported entities.ftm.json to initial diff version"""
        with self._store.open(path.entities_diff(version, ts), "wb") as o:
            for data in self._store.stream(path.ENTITIES_JSON):
                line = orjson.dumps(
                    self._make_envelope(data), option=orjson.OPT_APPEND_NEWLINE
                )
                o.write(line)
