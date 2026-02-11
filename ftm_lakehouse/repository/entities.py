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
    ARROW_SCHEMA,
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
from ftm_lakehouse.storage.parquet import PARTITIONS, SIDECAR_SCHEMA

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

    def _make_dedup_connection(self) -> duckdb.DuckDBPyConnection | None:
        """Create a DuckDB connection with a temp table of existing statement IDs.

        Returns None if the main parquet table doesn't exist yet (first flush).
        """
        try:
            dt = self._statements._store.deltatable
        except TableNotFoundError:
            return None
        con = duckdb.connect()
        con.register("arrow", dt.to_pyarrow_dataset())
        con.execute("CREATE TEMP TABLE existing_ids AS SELECT DISTINCT id FROM arrow")
        return con

    def flush(self) -> int:
        """
        Flush statements from journal to parquet store.

        Uses dedup logic:
        - New statements (not in main table): append to main + insert into sidecar
        - Duplicate statements (already in main): update sidecar last_seen only
        - Tombstones (deleted_at set): update sidecar deleted_at only

        Returns:
            Number of new statements flushed to the main table
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

            con = self._make_dedup_connection()

            total_new = 0
            current_bucket: str | None = None
            current_origin: str | None = None
            current_batch: list[dict] = []

            for _, bucket, origin, _, data, deleted_at in self._journal.flush():
                # Write batch when partition changes
                if bucket != current_bucket or origin != current_origin:
                    if current_batch:
                        total_new += self._write_flush_batch(current_batch, con)
                    current_batch = []
                    current_bucket = bucket
                    current_origin = origin

                if deleted_at is not None:
                    row = unpack_tombstone_row(data)
                    row["_deleted_at"] = deleted_at
                else:
                    stmt = unpack_statement(data)
                    row = lake_pack_statement(stmt)
                current_batch.append(row)

            # Write final batch
            if current_batch:
                total_new += self._write_flush_batch(current_batch, con)

            self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Flushed statements from journal to lake",
                count=total_new,
                took=t.took,
                journal=self._journal.uri,
            )

            return total_new

    def _write_flush_batch(
        self, batch: list[dict], con: duckdb.DuckDBPyConnection | None
    ) -> int:
        """Write a partition batch with three-way split.

        1. Tombstones → sidecar mark_deleted only
        2. New rows (anti-join with existing IDs) → write main table + sidecar upsert
        3. Duplicate rows (semi-join) → sidecar upsert only (updates last_seen)

        Returns:
            Number of new rows written to main table
        """
        now = datetime.now(timezone.utc)

        # Split tombstones from live rows
        tombstones = [r for r in batch if r.get("_deleted_at") is not None]
        live = [r for r in batch if r.get("_deleted_at") is None]

        # Handle tombstones → sidecar only
        if tombstones:
            tomb_ids = [r["id"] for r in tombstones]
            tomb_deleted_at = [r["_deleted_at"] for r in tombstones]
            tomb_table = pa.table(
                {
                    "id": pa.array(tomb_ids, type=pa.string()),
                    "deleted_at": pa.array(tomb_deleted_at, type=pa.timestamp("us")),
                }
            )
            self._statements._sidecar.mark_deleted(tomb_table)

        if not live:
            return 0

        # Build sidecar rows for all live statements
        live_ids = [r["id"] for r in live]
        live_first_seen = [r.get("first_seen") or now for r in live]
        live_last_seen = [r.get("last_seen") or now for r in live]

        if con is not None:
            # Determine which are new vs duplicates
            batch_ids_table = pa.table({"id": pa.array(live_ids, type=pa.string())})
            con.register("batch_ids", batch_ids_table)

            new_ids_result = con.execute(
                "SELECT b.id FROM batch_ids b "
                "LEFT JOIN existing_ids e ON b.id = e.id "
                "WHERE e.id IS NULL"
            ).fetchall()
            new_id_set = {r[0] for r in new_ids_result}
            con.unregister("batch_ids")

            new_rows = [r for r in live if r["id"] in new_id_set]
        else:
            # First flush — all rows are new
            new_rows = live

        # Upsert all live rows into sidecar (new + dupes)
        sidecar_table = pa.table(
            {
                "id": pa.array(live_ids, type=pa.string()),
                "first_seen": pa.array(live_first_seen, type=pa.timestamp("us")),
                "last_seen": pa.array(live_last_seen, type=pa.timestamp("us")),
                "deleted_at": pa.array([None] * len(live_ids), type=pa.timestamp("us")),
            },
            schema=SIDECAR_SCHEMA,
        )
        self._statements._sidecar.upsert(sidecar_table)

        # Write only new rows to main table
        if new_rows:
            table = pa.Table.from_pylist(new_rows, schema=ARROW_SCHEMA)
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

            # Update dedup connection for subsequent batches in the same flush
            if con is not None:
                new_ids_arr = pa.table(
                    {"id": pa.array([r["id"] for r in new_rows], type=pa.string())}
                )
                con.register("_new_batch", new_ids_arr)
                con.execute("INSERT INTO existing_ids SELECT id FROM _new_batch")
                con.unregister("_new_batch")

        return len(new_rows)

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
        """Read all statements for an entity from parquet + journal.

        Uses sidecar join when available to filter already-deleted statements.
        """
        stmts_by_id: dict[str, Statement] = {}

        # Read from parquet (with sidecar filtering if available)
        try:
            dt = self._statements._store.deltatable
            sidecar = self._statements._sidecar

            if sidecar.exists:
                sidecar_dt = sidecar.deltatable
                con = duckdb.connect()
                con.register("arrow", dt.to_pyarrow_dataset())
                con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
                result = con.sql(
                    "SELECT arrow.* FROM arrow "
                    "JOIN sidecar sc ON arrow.id = sc.id "
                    "WHERE sc.deleted_at IS NULL "
                    f"AND arrow.entity_id = '{entity_id}'"
                )
            else:
                rel = duckdb.arrow(dt.to_pyarrow_dataset())
                result = rel.query(
                    "arrow",
                    f"SELECT * FROM arrow WHERE entity_id = '{entity_id}'",
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
