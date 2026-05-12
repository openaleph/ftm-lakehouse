"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

import csv
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from typing import Generator, Iterable, Iterator, cast

import orjson
import pyarrow as pa
from anystore.io import smart_open, smart_write_json
from anystore.store import get_store
from anystore.types import SDict, Uri
from anystore.util import Took, mask_uri
from followthemoney import EntityProxy, Statement, StatementEntity
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import pack_statement
from ftmq.types import StatementEntities, Statements, ValueEntities
from sqlalchemy import select

from ftm_lakehouse.core.api import api_delegate, no_api
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.logic.entities.aggregate import EntityPayload, aggregate_unsafe
from ftm_lakehouse.logic.parquet import QUERY_IN_BATCH_SIZE
from ftm_lakehouse.model.statement import SHARDED_SCHEMA
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.repository.diff import ParquetDiffMixin
from ftm_lakehouse.repository.entities.api import ApiEntityRepository
from ftm_lakehouse.storage.journal import get_journal
from ftm_lakehouse.storage.journal.base import BaseJournalWriter
from ftm_lakehouse.storage.journal.sql import SqlJournalStore
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.util import make_envelope

settings = Settings()


class EntityRepository(ParquetDiffMixin, BaseRepository, ApiEntityRepository):
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
        with repo.writer(origin="import") as writer:
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
        shards: int | None = None,
    ) -> None:
        super().__init__(dataset, uri)
        self.shards = shards if shards is not None else settings.entity_shards
        self._journal = get_journal(dataset)
        self._statements = ParquetStore(uri, dataset, self.shards)
        self._store = get_store(self._store_uri)

    @contextmanager
    def writer(
        self, origin: str | None = None
    ) -> Generator[BaseJournalWriter, None, None]:
        """
        Get a bulk writer for adding entities/statements.

        Usage:
            with repo.writer(origin="import") as writer:
                writer.add_entity(entity)
        """
        with self._tags.touch(tag.JOURNAL_UPDATED):
            writer = self._journal.writer(self.shards, origin)
            try:
                yield writer
            except BaseException:
                writer.rollback()
                raise
            else:
                writer.flush()
            finally:
                writer.close()
                # keep journal not too full
                if self._journal.count() >= 1_000_000:
                    self.flush()

    def add(self, entity: EntityProxy, origin: str | None = None) -> None:
        """Add a single entity to the journal."""
        self.add_many([entity], origin)

    def add_many(
        self, entities: Iterable[EntityProxy], origin: str | None = None
    ) -> None:
        """Add an entity iterator to the journal."""
        with self.writer(origin) as writer:
            for entity in entities:
                writer.add_entity(entity)
        # keep journal not too full
        if self._journal.count() >= 1_000_000:
            self.flush()

    @api_delegate("_api_flush")
    def flush(self) -> int:
        """
        Flush statements from journal to parquet store.

        Groups journal rows by ``(shard, bucket, origin)`` and appends one
        sorted parquet file per partition. Duplicates and tombstones land as
        new rows; call ``merge`` afterwards to collapse them.

        Returns:
            Number of statements appended
        """
        if self._journal.count() == 0:
            self.log.debug("Journal is empty", journal=mask_uri(self._journal.uri))
            # set tags for the initial run
            if not self._tags.exists(tag.JOURNAL_FLUSHED):
                self._tags.set(tag.JOURNAL_FLUSHED)
            if not self._tags.exists(tag.STATEMENTS_UPDATED):
                self._tags.set(tag.STATEMENTS_UPDATED)
            return 0

        with (
            self._tags.touch(tag.JOURNAL_FLUSHED),
            self._tags.touch(tag.STATEMENTS_UPDATED),
            Took() as t,
        ):
            self.log.info("Flushing journal ...", journal=mask_uri(self._journal.uri))

            now = datetime.now(timezone.utc)
            total = 0
            current_shard: str | None = None
            buffer: list[dict] = []

            def _flush_buffer() -> None:
                nonlocal total
                if not buffer:
                    return
                batch = pa.Table.from_pylist(buffer, schema=SHARDED_SCHEMA)
                self._statements.append(batch)
                total += len(batch)
                buffer.clear()

            # The journal yields rows already ordered by ``shard``; stream
            # per shard so memory is bounded to one shard's worth at a time.
            # ``ParquetStore.append`` splits each per-shard batch by bucket.
            for journal_row in self._journal.flush():
                if current_shard is not None and current_shard != journal_row.shard:
                    _flush_buffer()
                current_shard = journal_row.shard

                stmt = unpack_statement(journal_row.data)
                row = pack_statement(stmt)
                row["first_seen"] = row.get("first_seen") or now
                row["deleted_at"] = journal_row.deleted_at
                # Tombstones bump last_seen to the delete timestamp so they
                # win the ``ROW_NUMBER() OVER (... ORDER BY last_seen DESC)``
                # tiebreak in merge() against the live row they replace.
                if journal_row.deleted_at is not None:
                    row["last_seen"] = journal_row.deleted_at
                else:
                    row["last_seen"] = row.get("last_seen") or now
                row["shard"] = journal_row.shard
                buffer.append(row)

            _flush_buffer()

            self.log.info(
                "Flushed statements from journal to lake",
                count=total,
                took=t.took,
                journal=mask_uri(self._journal.uri),
            )

            return total

    @api_delegate("_api_merge")
    def merge(self, grace_period_days: int | None = None) -> None:
        """Collapse duplicates and reap expired tombstones from parquet store"""
        self._statements.merge(grace_period_days)

    @api_delegate("_api_query")
    def query(
        self,
        entity_ids: Iterable[str] | None = None,
        flush_first: bool = False,
        **filters,
    ) -> StatementEntities:
        """
        Query entities from the parquet store.

        Additional filter kwargs are passed to ftmq Query.

        Args:
            entity_ids: Filter by entity IDs
            flush_first: Flush journal before querying (default False)

        Yields:
            StatementEntity objects matching the query
        """
        if flush_first:
            self.flush()

        if entity_ids:
            filters["entity_id__in"] = list(entity_ids)
        q = Query().where(**filters)

        yield from self._statements.query(q)

    @api_delegate("_api_query_statements")
    def query_statements(self, q: Query | None = None) -> Statements:
        q = q or Query()
        sql = q.sql.statements
        yield from self._statements.query_statements(sql)

    def get(
        self,
        entity_id: str,
        origin: str | None = None,
        flush_first: bool = False,
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
        if self._store.exists(path.ENTITIES_JSON):
            uri = self._store.to_uri(path.ENTITIES_JSON)
            yield from smart_read_proxies(uri)

    @no_api
    def export_entities(
        self, output_uri: str, statements_csv_uri: str | None = None
    ) -> None:
        """
        Export entities to a JSON lines file without FtM object construction.

        Uses aggregate_unsafe() to bypass Statement/StatementEntity/to_dict()
        and writes directly to orjson output.

        When ``statements_csv_uri`` is provided (e.g. from ``make --full``
        where statements.csv was just exported), reads the already-sorted CSV
        instead of re-scanning the parquet store.

        Args:
            output_uri: Destination URI for the entities.ftm.json file
            statements_csv_uri: Optional path to a fresh, sorted statements.csv
        """
        self._store.ensure_parent(path.ENTITIES_JSON)

        if statements_csv_uri is not None:
            rows = self._stream_statements_csv(statements_csv_uri)
        else:
            rows = self._statements._query_statement_data()

        with smart_open(output_uri, mode="wb") as fh:
            for entity in rows:
                fh.write(orjson.dumps(entity, option=orjson.OPT_APPEND_NEWLINE))

    def _stream_statements_csv(self, csv_uri: str) -> Iterator[EntityPayload]:
        """Stream entity dicts from an already-sorted statements CSV.

        The CSV is already sorted by canonical_id and has timestamps
        baked in, so no DuckDB sort is needed — just a sequential read
        through aggregate_unsafe().
        """
        with smart_open(csv_uri, mode="r") as fh:
            yield from aggregate_unsafe(csv.DictReader(fh))

    @api_delegate("_api_delete_entity")
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
        with self._journal.writer(self.shards) as w:
            for stmt in stmts:
                w.add_statement(stmt, deleted_at=now)
        self._tags.set(tag.JOURNAL_UPDATED)
        return len(stmts)

    def delete_statement(self, stmt: Statement) -> None:
        """Delete a single statement via journal tombstone.

        Args:
            stmt: The Statement to delete
        """
        with self._tags.touch(tag.JOURNAL_UPDATED):
            now = datetime.now(timezone.utc)
            with self._journal.writer(self.shards) as w:
                w.add_statement(stmt, deleted_at=now)

    @no_api
    def _collect_entity_statements(self, entity_id: str) -> list[Statement]:
        """Read all statements for an entity from parquet + journal.

        Uses shard-partitioned query for efficient single-entity lookup.
        """
        stmts_by_id: dict[str, Statement] = {}
        journal = cast(SqlJournalStore, self._journal)

        # Read from parquet store (uses shard partition for pruning)
        for stmt in self._statements.get_statements(entity_id):
            if stmt.id:
                stmts_by_id[stmt.id] = stmt

        # Read from journal (may override parquet entries). Use the shard
        # index for an index-assisted scan, then filter by canonical_id in
        # the unpacked statement.
        shard = path.entity_shard(entity_id, self.shards)
        q = (
            select(journal.table)
            .where(journal.table.c.shard == shard)
            .where(journal.table.c.deleted_at.is_(None))
        )
        with journal.engine.connect() as conn:
            for row in conn.execute(q):
                stmt = unpack_statement(row.data)
                if stmt.entity_id != entity_id:
                    continue
                if stmt.id:
                    stmts_by_id[stmt.id] = stmt

        return list(stmts_by_id.values())

    @api_delegate("_api_stats")
    def get_statistics(self) -> DatasetStats:
        """Compute statistics from the parquet store."""
        return self._statements.stats()

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        if self._is_api:
            return self._api_version()
        return self._statements.version

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_ENTITIES

    @no_api
    def _get_changed_ids(self, since: datetime) -> Iterator[str]:
        """Get entity IDs with statements added since the given timestamp."""
        return self._statements.get_changed_entity_ids(since)

    @no_api
    def _write_diff(self, entity_ids: Iterator[str], ts: datetime, **kwargs) -> str:
        """Write entities as line-based JSON with operation envelopes."""
        key = path.entities_diff(ts)
        with self._store.open(key, "wb") as o:
            smart_write_json(o, self._get_delta_entities(entity_ids))
        return self._store.to_uri(key)

    @no_api
    def _get_delta_entities(
        self, entity_ids: Iterator[str]
    ) -> Generator[SDict, None, None]:
        original_ids: set[str] = set()
        seen_ids: set[str] = set()
        it = iter(entity_ids)
        while batch := set(islice(it, QUERY_IN_BATCH_SIZE)):
            original_ids.update(batch)
            for entity in self.query(entity_ids=batch, flush_first=False):
                if entity.id:
                    seen_ids.add(entity.id)
                yield make_envelope(entity.to_dict())
        for entity_id in original_ids - seen_ids:
            yield make_envelope({"id": entity_id}, op="DEL")

    @no_api
    def _write_initial_diff(self, ts: datetime, **kwargs) -> None:
        """Copy over exported entities.ftm.json to initial diff version"""
        with self._store.open(path.entities_diff(ts), "wb") as o:
            for data in self._store.stream(path.ENTITIES_JSON):
                line = orjson.dumps(
                    make_envelope(data), option=orjson.OPT_APPEND_NEWLINE
                )
                o.write(line)
