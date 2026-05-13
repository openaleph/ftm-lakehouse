"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from typing import Generator, Iterable, Iterator, cast

import orjson
import pyarrow as pa
from anystore.io import smart_write_json
from anystore.io.read import smart_stream_csv
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
from ftm_lakehouse.logic.entities.aggregate import aggregate_unsafe
from ftm_lakehouse.logic.parquet import QUERY_IN_BATCH_SIZE
from ftm_lakehouse.model.statement import SHARDED_SCHEMA, StatementRow
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

    @api_delegate("_api_flush")
    def flush(self) -> int:
        """Drain the journal into the parquet statement store.

        Groups journal rows by ``(shard, bucket, origin)`` and appends one
        sorted parquet file per partition via :meth:`write_statements`.
        Duplicates and tombstones land as new rows; call :meth:`merge`
        afterwards to collapse them.

        Returns:
            Number of statements appended.
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
            total = self.write_statements(self._journal.flush_statements(), now=now)

            self.log.info(
                "Flushed statements from journal to lake",
                count=total,
                took=t.took,
                journal=mask_uri(self._journal.uri),
            )

            return total

    @no_api
    def write_statements(
        self,
        statements: Iterable[StatementRow],
        now: datetime | None = None,
    ) -> int:
        """Pack and append a shard-sorted stream of statements to parquet.

        Input is an iterable of :class:`StatementRow` already ordered by
        shard – exactly what :meth:`EntityBuffer.flush_buffer` and
        :meth:`JournalStore.flush_statements` produce. Consecutive rows for
        the same shard accumulate into one per-shard batch;
        :meth:`ParquetStore.append` then splits each batch by bucket and
        writes one parquet file per partition.

        This is the shared core of:

        - :meth:`flush` (drains the journal),
        - bare bulk-import paths in the CLI that bypass the journal entirely.

        Tombstones (rows with ``deleted_at`` set) get their ``last_seen``
        bumped to ``deleted_at`` so they win the ``ROW_NUMBER() OVER (... ORDER
        BY last_seen DESC)`` tiebreak against the live row in
        :meth:`ParquetStore.merge`.

        Args:
            statements: Shard-sorted stream of :class:`StatementRow`.
            now: Default timestamp for missing ``first_seen`` /
                ``last_seen``. Defaults to the current UTC time.

        Returns:
            Number of statements written.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        total = 0
        current_shard: str | None = None
        buffer: list[dict] = []

        def _emit() -> None:
            nonlocal total
            if not buffer:
                return
            batch = pa.Table.from_pylist(buffer, schema=SHARDED_SCHEMA)
            self._statements.append(batch)
            total += len(batch)
            buffer.clear()

        for row in statements:
            if current_shard is not None and current_shard != row.shard:
                _emit()
            current_shard = row.shard

            data = pack_statement(row.stmt)
            data["first_seen"] = data.get("first_seen") or now
            data["deleted_at"] = row.deleted_at
            # Tombstones bump last_seen to the delete timestamp so they win
            # the ROW_NUMBER ORDER BY last_seen DESC tiebreak in merge().
            data["last_seen"] = row.deleted_at or data.get("last_seen") or now
            data["shard"] = row.shard
            buffer.append(data)

        _emit()
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
            with self._store.open(path.ENTITIES_JSON) as fh:
                yield from smart_read_proxies(fh)

    @no_api
    def export_entities(self, statements_csv_uri: str | None = None) -> None:
        """
        Export entities to a JSON lines file without FtM object construction.

        Uses aggregate_unsafe() to bypass Statement/StatementEntity/to_dict()
        and writes directly to orjson output.

        When ``statements_csv_uri`` is provided (e.g. from ``make --full``
        where statements.csv was just exported), reads the already-sorted CSV
        instead of re-scanning the parquet store.

        Args:
            statements_csv_uri: Optional path to a fresh, sorted statements.csv
        """
        self._store.ensure_parent(path.ENTITIES_JSON)

        if statements_csv_uri is not None:
            rows = smart_stream_csv(statements_csv_uri)
        else:
            rows = self._statements._query_statement_data()

        entities = aggregate_unsafe(rows, self.dataset)
        entities = (e.to_dict() for e in entities)

        with self._store.open(path.ENTITIES_JSON, "wb") as fh:
            smart_write_json(fh, entities)

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
