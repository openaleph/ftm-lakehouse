"""EntityRepository – entity/statement operations over JournalStore + ParquetStore."""

from datetime import datetime
from functools import cached_property
from typing import Iterable, Iterator, cast

import pyarrow as pa
from anystore.types import Uri
from anystore.util import Took, mask_uri
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.statement import StatementDict
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import M, Query
from ftmq.types import StatementEntities, Statements, ValueEntities
from rigour.time import utc_now

from ftm_lakehouse.model.statement import LakehouseStatement
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

        for entity in repo.query(Query(C(origin="import"))):
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
    def statements(self) -> ParquetStore:
        """The dataset's parquet store, built lazily; raises in api mode."""
        if self._is_api:
            raise RuntimeError(
                f"`{type(self).__name__}.statements` is not available in API mode"
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

    def write_batches(self, tables: Iterable[pa.Table]) -> int:
        """Append `JOURNAL_SCHEMA` tables to parquet – the write loop of the
        journal drain and the bulk imports. Each table is durable before the
        next is pulled.

        Args:
            tables: Stream of packed statement tables.

        Returns:
            Number of rows written.
        """
        store = self.statements  # raises in api mode, also for no tables
        total = 0
        for table in tables:
            if not table.num_rows:
                continue
            store.append(table)
            total += table.num_rows
        return total

    def merge(self, force: bool = False) -> None:
        """Flush, then [`merge`][ParquetStore.merge] the parquet store – ``force``
        rewrites clean partitions too."""
        store = self.statements  # raises in api mode, ahead of the flush
        self.flush()
        store.merge(force)

    def shard(self, shards: int) -> None:
        """Flush, then [`shard`][ParquetStore.shard] the parquet store – the
        storage half; [`ShardOperation`][ftm_lakehouse.operation.maintenance.ShardOperation]
        writes the new count to ``config.yml``.

        Args:
            shards: Target shard count; ``<= 1`` means a single shard.
        """
        store = self.statements  # raises in api mode, ahead of the flush
        self.flush()
        store.shard(shards)
        self.shards = shards

    def query_statements_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """[`query_statements`][EntityRepository.query_statements] as plain dicts –
        the stored columns, timestamps as ISO strings, as the api sends them."""
        for row in self.statements._statement_data(q):
            yield cast(
                StatementDict,
                {
                    k: v.isoformat() if isinstance(v, datetime) else v
                    for k, v in row.items()
                },
            )

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
        yield from self.statements.query(q)

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
        yield from self.statements.query_statements(q)

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
        self.statements.delete_origin(origin)

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
        return self.statements.stats()

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        return self.statements.version
