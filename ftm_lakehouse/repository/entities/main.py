"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from contextlib import contextmanager
from datetime import datetime
from functools import cached_property
from typing import Generator, Iterable, Iterator, cast

import pyarrow as pa
from anystore.types import Uri
from anystore.util import Took, mask_uri
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.statement import StatementDict
from ftmq import C
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import M, Query
from ftmq.types import StatementEntities, Statements, ValueEntities
from rigour.time import utc_now

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.settings import Settings
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

settings = Settings()


class EntityRepository(DatasetHandle):
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
        return ParquetStore(self.uri, self.dataset, self.shards, self.compression)

    @contextmanager
    def writer(
        self, origin: str | None = None, role: str | None = None
    ) -> Generator[BaseJournalWriter, None, None]:
        """Get a bulk writer for adding entities/statements.

        The writer owns its own lifecycle (insert the tail on success, drop
        the un-inserted buffer on error, close either way – see
        `BaseJournalWriter.__exit__`); this adds the freshness tag,
        stamped only when the block leaves cleanly.

        Example:
            ```python
            with repo.writer(origin="import") as writer:
                writer.add_entity(entity)
            ```

        Args:
            origin: Origin tag for statements written through this writer.
            role: Default role – who is asserting these statements – for
                statements that carry none of their own.

        Yields:
            The journal writer, open for the duration of the block.
        """
        with (
            self._tags.touch(tag.JOURNAL_UPDATED),
            self._journal.writer(origin, role) as writer,
        ):
            yield writer

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
        """Drain the journal into the parquet statement store.

        The journal holds the parquet statement columns, so this streams Arrow
        batches from one store into the other via
        [`write_batches`][EntityRepository.write_batches]. Duplicates and
        tombstones land as new rows; call [`merge`][EntityRepository.merge]
        afterwards to collapse them.

        Returns:
            Number of statements appended.
        """
        with self._tags.touch(tag.JOURNAL_FLUSHED), Took() as t:
            self.log.info("Flushing journal ...", journal=mask_uri(self._journal.uri))
            total = self.write_batches(self._journal.flush_batches())

        if total:
            self.log.info(
                "Flushed statements from journal to lake",
                count=total,
                took=t.took,
                journal=mask_uri(self._journal.uri),
            )
        elif not self._tags.exists(tag.STATEMENTS_OPTIMIZED):
            # initial run: give freshness comparisons a baseline. An empty
            # store is trivially canonical, and without the tag every
            # consumer keyed on it would re-run forever (`is_latest` is
            # False when no dependency exists at all).
            self._tags.set(tag.STATEMENTS_OPTIMIZED)
        return total

    @no_api
    def write_batches(self, tables: Iterable[pa.Table]) -> int:
        """Append packed Arrow tables to parquet – the one write loop.

        Every producer packs its own rows and hands them here: the journal
        drain (`JournalStore.flush_batches`), the safe bulk import
        (`flush_table`)
        and the unsafe one
        (`RowBuffer`). Tables
        arrive in `JOURNAL_SCHEMA` – no
        ``shard`` column, [`ParquetStore.append`][ParquetStore.append] derives it – and go
        straight there; one is durable before the producer is asked for the
        next, which is what lets the journal drop a segment it has handed
        over. Sizing is the producer's call: each table becomes one parquet
        file per ``(shard, bucket, origin)`` partition it spans, so bigger
        tables cost fewer files and fewer Delta commits.

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
        """Collapse duplicates and reap expired tombstones from parquet store.

        Flushes the journal first. ``force`` rewrites every partition
        regardless of freshness tags.
        """
        self.flush()
        self._statements.merge(force)

    @no_api
    def shard(self, shards: int) -> None:
        """Re-shard the parquet store onto ``shards`` entity-hash shards.

        Drains the journal first – a row left in it would be placed by
        whichever count the flushing store resolves, and only rows already
        in parquet are moved by the rewrite – then rewrites the store
        ([`ParquetStore.shard`][ParquetStore.shard]) and adopts the new count, so this
        instance keeps resolving reads and writes to the right shards.

        Only the storage half: the dataset's ``config.yml`` is what every
        *other* reader resolves the count from, and
        [`ShardOperation`][ftm_lakehouse.operation.maintenance.ShardOperation] writes
        it once this returns.

        Args:
            shards: Target shard count; ``<= 1`` means a single shard.
        """
        self.flush()
        self._statements.shard(shards)
        self.shards = shards

    @no_api
    def compact(self) -> None:
        """Bin-pack small parquet files within each partition."""
        self._statements.compact()

    @no_api
    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files tombstoned in the Delta log."""
        self._statements.vacuum(retention_hours=retention_hours)

    @no_api
    def sweep(
        self, with_csv_export: bool = True, tee: bool = True
    ) -> Iterator[StatementDict]:
        """One scan of the store, optionally writing ``statements.csv`` from it.

        Delegates to [`ParquetStore.sweep`][ParquetStore.sweep] with this
        dataset's csv key, so the artifact carries the configured codec.

        Args:
            with_csv_export: Write the ``statements.csv`` artifact from the same
                Arrow batches the rows come from.
            tee: Yield row dicts. ``False`` keeps the scan columnar.

        Yields:
            ``StatementDict`` rows, unless ``tee`` is off.
        """
        key = self.EXPORTS_STATEMENTS if with_csv_export else None
        yield from self._statements.sweep(key, tee)

    @property
    @no_api
    def exists(self) -> bool:
        """Whether the statement store has been written – local only."""
        return self._statements.exists

    @property
    @no_api
    def needs_merge(self) -> bool:
        """Whether the statement store has writes that
        [`merge`][EntityRepository.merge] has not collapsed yet – local only.

        Reads are canonical only on a merged store, so anything publishing
        canonical rows (the exports, and their diffs strictly) checks
        this first. See [`ParquetStore.needs_merge`][ParquetStore.needs_merge].
        """
        return self._statements.needs_merge

    def query_statements_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Query raw statement dicts from the parquet store.

        The fast read: no `LakehouseStatement` construction – use
        [`query_statements`][EntityRepository.query_statements] for model
        objects. Same execution strategy as
        [`query_statements`][EntityRepository.query_statements], so a sorted or
        sliced query still runs globally instead of once per partition.
        """
        yield from self._statements._statement_data(q)

    @no_api
    def evolve_schema(self) -> list[str]:
        """Add statement columns the parquet store was created without.

        Delegates to [`ParquetStore.evolve_schema`][ParquetStore.evolve_schema],
        the primitive behind the schema migrations.

        Returns:
            Names of the columns added – empty if the store is already current.
        """
        return self._statements.evolve_schema()

    @no_api
    def unlock(self) -> bool:
        """Forcibly release the dataset write fence.

        Delegates to [`ParquetStore.unlock`][ParquetStore.unlock]. Use as an operator
        escape hatch when a writer died with the lock held; do not
        invoke while a legitimate writer is still running.

        Returns:
            ``True`` if a lock was released, ``False`` otherwise.
        """
        return self._statements.unlock()

    def query(
        self, q: Query | None = None, *, flush_first: bool = False
    ) -> StatementEntities:
        """Query entities from the parquet store.

        Args:
            q: ftmq ``Query`` of entity-level filters (schema, properties, ...).
            flush_first: Flush the journal to parquet before querying.

        Yields:
            StatementEntity objects matching the query.
        """
        if flush_first:
            self.flush()
        yield from self._statements.query(q)

    def query_statements(
        self, q: Query | None = None, *, flush_first: bool = False
    ) -> Statements:
        """Query statements from the parquet store.

        Args:
            q: ftmq ``Query`` – filters plus ordering / slicing.
            flush_first: Flush the journal to parquet before querying.

        Yields:
            `LakehouseStatement` objects.
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
        """
        Stream entities from the exported JSON file.

        This reads from the pre-exported entities.ftm.json file,
        not directly from the parquet store – decoded with the dataset's
        codec, since that artifact is written compressed when configured.
        """
        if self._store.exists(self.ENTITIES_JSON):
            with self._store.open(
                self.ENTITIES_JSON, "rb", compression=self.compression
            ) as raw:
                yield from smart_read_proxies(raw)

    def delete_entity(self, entity_id: str, origin: str | None = None) -> int:
        """Delete all statements for an entity via journal tombstones.

        Reads statements from both parquet and journal, then UPSERTs
        tombstone rows (with deleted_at set) into the journal. Each
        tombstone carries the live row's ``fragment`` and ``role`` – both
        are row identity, so a tombstone missing either lands in a different
        merge group and shadows nothing. Reading the live rows first is what
        makes that automatic: one tombstone per row means every role's
        assertion is deleted, which is what deleting the entity means.

        Args:
            entity_id: The entity ID to delete
            origin: Only delete entity data from this origin

        Returns:
            Number of tombstone statements written
        """
        now = utc_now()
        stmts = self._collect_entity_statements(entity_id)
        if not stmts:
            return 0
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
        """Delete a single statement via journal tombstone.

        Args:
            stmt: The Statement to delete. A
                `ftm_lakehouse.model.statement.LakehouseStatement` (e.g. read
                back via `ParquetStore.get_statements`) carries its own
                fragment and role.
            fragment: Fragment override – required to shadow a
                fragment-bearing row when passing a plain ``Statement``;
                leave unset otherwise.
            role: Role override – likewise required to shadow a row written
                under a role when passing a plain ``Statement``.
        """
        with self.writer() as w:
            w.add_statement(stmt, deleted_at=utc_now(), fragment=fragment, role=role)

    def delete_origin(self, origin: str) -> None:
        """Physically delete an entire origin – journal included.

        Unlike [`delete_entity`][EntityRepository.delete_entity] this writes no
        tombstones: ``origin`` is a partition column, so
        [`ParquetStore.delete_origin`][ParquetStore.delete_origin] drops whole
        partitions under the maintenance fence and the rows are gone at that
        commit – no merge, no grace period.

        The journal is flushed first, so rows written under ``origin`` and
        still buffered land in parquet in time to be dropped rather than
        resurrecting on the next [`flush`][EntityRepository.flush]. Flushing
        happens *outside* the fence – its append takes the shared side, which
        the exclusive one locks out – so a writer journalling into ``origin``
        during the drop still survives it. Stop the writers to be sure.

        Args:
            origin: The origin tag to drop.

        Raises:
            ValueError: If ``origin`` is not a safe origin name
                (see `validate_origin`).
            RuntimeError: When the write fence cannot be acquired.
        """
        # validate before flushing – a bad origin must cost nothing
        origin = validate_origin(origin)
        self.flush()
        self._statements.delete_origin(origin)

    @no_api
    def _collect_entity_statements(self, entity_id: str) -> list[LakehouseStatement]:
        """Read all statements for an entity from parquet + journal.

        Uses shard-partitioned query for efficient single-entity lookup.
        Statements are keyed by
        `ftm_lakehouse.model.statement.LakehouseStatement.dedupe_key` – the
        same statement content under distinct fragments, origins or roles is
        distinct for tombstoning purposes, so each live row gets its own
        matching tombstone.
        """
        stmts_by_key: dict[str, LakehouseStatement] = {}

        q = Query(M(entity_id=entity_id))
        for stmt in self.query_statements(q):
            stmt = cast(LakehouseStatement, stmt)
            if stmt.id:
                stmts_by_key[stmt.dedupe_key] = stmt

        # Read from journal (may override parquet entries) – typed columns,
        # so the entity filter runs in SQL.
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
    def deleted_ids(self, since: datetime) -> Iterator[str]:
        """Entity ids with statements tombstoned since the given timestamp.

        Reads `ParquetStore.source_raw`, since the live view hides exactly
        the rows this asks about
        """
        q = Query(C(deleted_at__gte=since))
        return self._statements.get_entity_ids(q, source=self._statements.source_raw)
