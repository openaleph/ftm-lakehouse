"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from datetime import datetime
from functools import cached_property
from typing import Iterable, Iterator, cast

import pyarrow as pa
from anystore.interface.lock import Lock
from anystore.types import Uri
from anystore.util import Took, mask_uri
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.statement import StatementDict
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import M, Query
from ftmq.types import StatementEntities, Statements, ValueEntities
from rigour.time import utc_now

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.model.statement import DeleteCandidate, LakehouseStatement
from ftm_lakehouse.repository.artifacts import (
    EntitiesArtifact,
    StatementsArtifact,
)
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.storage.journal import get_journal
from ftm_lakehouse.storage.journal.base import BaseJournalWriter
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.util import validate_origin


class EntityRepository(DatasetHandle):
    """Entities and statements of a dataset: writes go to the journal, reads to
    the parquet store, [`flush`][EntityRepository.flush] moves rows between them.

    Example:
        ```python
        repo = get_entities("my_data")

        with repo.writer(origin="import") as writer:
            writer.add_entity(entity)
        repo.flush()

        for entity in repo.query(Query(M(origin="import"))):
            process(entity)
        ```
    """

    def __init__(
        self,
        dataset: str,
        uri: Uri,
    ) -> None:
        super().__init__(dataset, uri)
        if self._is_api and type(self) is EntityRepository:
            raise RuntimeError(
                "`EntityRepository` cannot run against an http uri directly "
                "– resolve the repository via `get_entities()`"
            )
        self.shards = self._model.shards
        self.compression = self._model.compression
        self._journal = get_journal(dataset)
        self.ENTITIES_JSON = EntitiesArtifact(self).key
        self.EXPORTS_STATEMENTS = StatementsArtifact(self).key

    @cached_property
    def _statements(self) -> ParquetStore:
        """Local parquet store, built lazily – api instances never get one."""
        if self._is_api:
            raise RuntimeError(
                f"`{type(self).__name__}._statements` is not available in API mode"
            )
        return ParquetStore(self.uri, self.dataset, self.shards)

    def writer(
        self, origin: str | None = None, role: str | None = None
    ) -> BaseJournalWriter:
        """A bulk journal writer – inserts its tail on success, drops it on error.

        Example:
            ```python
            with repo.writer(origin="import") as writer:
                writer.add_entity(entity)
            ```

        Args:
            origin: Origin of the statements written through it.
            role: Who asserts them, for statements that carry no role.
        """
        return self._journal.writer(origin, role)

    def add(
        self,
        entity: EntityProxy,
        origin: str | None = None,
        fragment: str | None = None,
        role: str | None = None,
    ) -> None:
        """Add a single entity to the journal."""
        self.add_many([entity], origin, fragment, role)

    def add_many(
        self,
        entities: Iterable[EntityProxy],
        origin: str | None = None,
        fragment: str | None = None,
        role: str | None = None,
    ) -> None:
        """Add an entity iterator to the journal."""
        with self.writer(origin, role) as writer:
            for entity in entities:
                writer.add_entity(entity, fragment=fragment)

    def flush(self) -> int:
        """Drain the journal into the parquet store.

        A concurrent flush makes this one a no-op, so ``0`` does not mean the
        journal is empty.

        Returns:
            Number of statements appended.
        """
        with Took() as t:
            self.log.info("Flushing journal ...", journal=mask_uri(self._journal.uri))
            total = self.write_batches(self._journal.flush_batches())
        if total:
            self.log.info(
                "Flushed statements from journal to lake",
                count=total,
                took=t.took,
                journal=mask_uri(self._journal.uri),
            )
        return total

    @no_api
    def write_batches(self, tables: Iterable[pa.Table]) -> int:
        """Append `JOURNAL_SCHEMA` tables to parquet – the write loop of the
        journal drain and both bulk imports. Each table is durable before the
        next is asked for; bigger tables mean fewer files.

        Args:
            tables: Stream of packed statement tables.

        Returns:
            Number of rows written.
        """
        total = 0
        for table in tables:
            if not table.num_rows:
                continue
            self._statements.append(table)
            total += table.num_rows
        return total

    def merge(self, force: bool = False) -> None:
        """Flush, then [`merge`][ParquetStore.merge] the parquet store – ``force``
        rewrites clean partitions too."""
        self.flush()
        self._statements.merge(force)

    @no_api
    def shard(self, shards: int) -> None:
        """Flush, then [`shard`][ParquetStore.shard] the parquet store – the
        storage half; [`ShardOperation`][ftm_lakehouse.operation.maintenance.ShardOperation]
        writes the new count to ``config.yml``.

        Args:
            shards: Target shard count; ``<= 1`` means a single shard.
        """
        self.flush()
        self._statements.shard(shards)
        self.shards = shards

    @no_api
    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete the parquet files the Delta log no longer references."""
        self._statements.vacuum(retention_hours=retention_hours)

    @no_api
    def merge_lock(self) -> Lock:
        """The store's [`merge_lock`][ParquetStore.merge_lock]."""
        return self._statements.merge_lock()

    @property
    @no_api
    def exists(self) -> bool:
        """Whether the parquet store exists."""
        return self._statements.exists

    @property
    @no_api
    def needs_merge(self) -> bool:
        """Whether an optimize has work ([`needs_merge`][ParquetStore.needs_merge])."""
        return self._statements.needs_merge

    def query_statements_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """[`query_statements`][EntityRepository.query_statements] as plain dicts."""
        yield from self._statements._statement_data(q)

    @no_api
    def evolve_schema(self) -> list[str]:
        """[`ParquetStore.evolve_schema`][ParquetStore.evolve_schema]."""
        return self._statements.evolve_schema()

    @no_api
    def configure_table(self) -> dict[str, str]:
        """[`ParquetStore.configure_table`][ParquetStore.configure_table]."""
        return self._statements.configure_table()

    @no_api
    def unlock(self) -> bool:
        """[`ParquetStore.unlock`][ParquetStore.unlock] – never while a writer
        is running."""
        return self._statements.unlock()

    def query(
        self, q: Query | None = None, *, flush_first: bool = False
    ) -> StatementEntities:
        """Query entities from the parquet store.

        Args:
            q: Filters, plus ordering / slicing.
            flush_first: Flush the journal first.
        """
        if flush_first:
            self.flush()
        yield from self._statements.query(q)

    def query_statements(
        self, q: Query | None = None, *, flush_first: bool = False
    ) -> Statements:
        """Query `LakehouseStatement` objects from the parquet store.

        Args:
            q: Filters, plus ordering / slicing.
            flush_first: Flush the journal first.
        """
        if flush_first:
            self.flush()
        yield from self._statements.query_statements(q)

    def get(self, entity_id: str, flush_first: bool = False) -> StatementEntity | None:
        """Get a single entity by ID."""
        q = Query(M(entity_id=entity_id))
        for entity in self.query(q, flush_first=flush_first):
            return entity
        return None

    def stream(self) -> ValueEntities:
        """Stream the exported ``entities.ftm.json`` – not the parquet store."""
        if self._store.exists(self.ENTITIES_JSON):
            with self._store.open(
                self.ENTITIES_JSON, "rb", compression=self.compression
            ) as raw:
                yield from smart_read_proxies(raw)

    def delete_entity(self, entity_id: str, origin: str | None = None) -> int:
        """Tombstone every statement of an entity, parquet and journal – one per
        live row, carrying its ``fragment`` and ``role`` so it shadows that row.

        Args:
            entity_id: The entity to delete.
            origin: Only delete its statements from this origin.

        Returns:
            Number of tombstones written.
        """
        now = utc_now()
        stmts = self._collect_entity_statements(entity_id)
        if origin:
            stmts = [s for s in stmts if s.origin == origin]
        with self.writer() as w:
            for stmt in stmts:
                w.add_statement(stmt, deleted_at=now)
        return len(stmts)

    def delete_statement(
        self,
        stmt: Statement,
        fragment: str | None = None,
        role: str | None = None,
    ) -> None:
        """Tombstone one statement.

        Args:
            stmt: The statement – a `LakehouseStatement` read back from
                [`query_statements`][EntityRepository.query_statements] carries
                its own fragment and role.
            fragment: The row's fragment, for a plain ``Statement``.
            role: The row's role, for a plain ``Statement``.
        """
        with self.writer() as w:
            w.add_statement(stmt, deleted_at=utc_now(), fragment=fragment, role=role)

    def delete_origin(self, origin: str) -> None:
        """Physically drop an origin: flush, then
        [`ParquetStore.delete_origin`][ParquetStore.delete_origin]. Rows journalled
        into it while this runs survive – stop the writers to be sure.

        Args:
            origin: The origin to drop.

        Raises:
            ValueError: If ``origin`` is not a safe origin name.
            RuntimeError: When the write fence cannot be acquired.
        """
        # validate before flushing – a bad origin must cost nothing
        origin = validate_origin(origin)
        self.flush()
        self._statements.delete_origin(origin)

    @no_api
    def _collect_entity_statements(self, entity_id: str) -> list[LakehouseStatement]:
        """An entity's statements from parquet and journal, one per
        ``dedupe_key`` – the journal's win."""
        stmts_by_key: dict[str, LakehouseStatement] = {}

        q = Query(M(entity_id=entity_id))
        for stmt in self.query_statements(q):
            stmt = cast(LakehouseStatement, stmt)
            if stmt.id:
                stmts_by_key[stmt.dedupe_key] = stmt

        for stmt in self._journal.iterate_entity(entity_id):
            if stmt.id:
                stmts_by_key[stmt.dedupe_key] = stmt

        return list(stmts_by_key.values())

    def stats(self) -> DatasetStats:
        """Compute statistics from the parquet store."""
        return self._statements.stats()

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        return self._statements.version

    @no_api
    def deleted_candidates(self, since: datetime) -> Iterator[DeleteCandidate]:
        """[`ParquetStore.deleted_candidates`][ParquetStore.deleted_candidates]."""
        return self._statements.deleted_candidates(since)
