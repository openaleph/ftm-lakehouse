"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

import csv
from contextlib import contextmanager
from datetime import datetime
from functools import cached_property
from typing import IO, Generator, Iterable, Iterator, cast

import orjson
import pyarrow as pa
from anystore.io import smart_open, smart_write_json
from anystore.types import SDict, Uri
from anystore.util import Took, mask_uri
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.statement import StatementDict
from ftmq import C
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import M, Query
from ftmq.store.lake import LakeStatement
from ftmq.types import StatementEntities, Statements, ValueEntities
from rigour.time import utc_now

from ftm_lakehouse.core.api import no_api
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.compress import compress_stream, decompress_stream
from ftm_lakehouse.logic.entities.aggregate import aggregate_unsafe
from ftm_lakehouse.model.statement import LakehouseStatement, statements_to_arrow
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.repository.diff import ParquetDiffMixin, make_envelope
from ftm_lakehouse.storage.journal import get_journal
from ftm_lakehouse.storage.journal.base import BaseJournalWriter, StatementTables
from ftm_lakehouse.storage.parquet import ParquetStore

settings = Settings()

WRITE_SHARD_BATCH = 100_000
"""Maximum rows accumulated per shard before an interim parquet write.

Prevents one giant shard from buffering arbitrarily many statements in memory
before :meth:`EntityRepository.write_statements` emits.
"""


class EntityRepository(ParquetDiffMixin, DatasetHandle):
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
        self.ENTITIES_JSON = path.entities_json(self.compression)
        self.EXPORTS_STATEMENTS = path.exports_statements(self.compression)

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

    def add(
        self,
        entity: EntityProxy,
        origin: str | None = None,
        fragment: str | None = None,
    ) -> None:
        """Add a single entity to the journal."""
        self.add_many([entity], origin, fragment)

    def add_many(
        self,
        entities: Iterable[EntityProxy],
        origin: str | None = None,
        fragment: str | None = None,
    ) -> None:
        """Add an entity iterator to the journal."""
        with self.writer(origin) as writer:
            for entity in entities:
                writer.add_entity(entity, fragment=fragment)

    def flush(self) -> int:
        """Drain the journal into the parquet statement store.

        The journal holds the parquet statement columns, so this streams Arrow
        batches from one store into the other via :meth:`write_batches`.
        Duplicates and tombstones land as new rows; call :meth:`merge`
        afterwards to collapse them.

        Returns:
            Number of statements appended.
        """
        with self._tags.touch(tag.JOURNAL_FLUSHED), Took() as t:
            self.log.info("Flushing journal ...", journal=mask_uri(self._journal.uri))
            total = self.write_batches(
                self._journal.flush_batches(ordered=self.shards > 1)
            )

        if total:
            self.log.info(
                "Flushed statements from journal to lake",
                count=total,
                took=t.took,
                journal=mask_uri(self._journal.uri),
            )
        elif not self._tags.exists(tag.STATEMENTS_UPDATED):
            # initial run: give freshness comparisons a baseline
            self._tags.set(tag.STATEMENTS_UPDATED)
        return total

    @no_api
    def write_statements(
        self,
        statements: Iterable[LakehouseStatement],
        now: datetime | None = None,
        batch_size: int | None = WRITE_SHARD_BATCH,
    ) -> int:
        """Pack and append a shard-sorted stream of statements to parquet.

        Input is an iterable of :class:`LakehouseStatement` already ordered
        by shard – exactly what :meth:`EntityBuffer.flush_buffer` produces.
        Consecutive statements of the same shard accumulate into one batch,
        which :func:`statements_to_arrow` packs columnwise before
        :meth:`write_batches` appends it.

        This is the safe bulk-import path (the CLI paths that bypass the
        journal). Rows that are packed already – the journal drain, the
        unsafe import – go to :meth:`write_batches` directly.

        Tombstones (rows with ``deleted_at`` set) get their ``last_seen``
        bumped to ``deleted_at`` in the packer so they win the ``ROW_NUMBER()
        OVER (... ORDER BY last_seen DESC)`` tiebreak in
        :meth:`ParquetStore.merge`.

        Args:
            statements: Shard-sorted stream of :class:`LakehouseStatement`.
            now: Default timestamp for missing ``first_seen`` /
                ``last_seen``. Defaults to the current UTC time.
            batch_size: Row cap per in-memory batch, or ``None`` to signal
                the caller already batches.

        Returns:
            Number of statements written.
        """
        return self.write_batches(
            self._pack_shards(statements, now or utc_now(), batch_size)
        )

    def _pack_shards(
        self,
        statements: Iterable[LakehouseStatement],
        now: datetime,
        batch_size: int | None,
    ) -> StatementTables:
        """Pack a shard-sorted statement stream into per-shard tables."""
        buffer: list[LakehouseStatement] = []

        def _pack() -> pa.Table:
            table = statements_to_arrow(buffer, now)
            buffer.clear()
            return table

        for stmt in statements:
            full = batch_size is not None and len(buffer) >= batch_size
            if buffer and (full or stmt.shard != buffer[-1].shard):
                yield _pack()
            buffer.append(stmt)

        if buffer:
            yield _pack()

    @no_api
    def write_batches(self, tables: StatementTables) -> int:
        """Append packed Arrow tables to parquet – the one write loop.

        Every path whose rows are packed already: the journal drain
        (:meth:`JournalStore.flush_batches`), the unsafe bulk import
        (:class:`~ftm_lakehouse.logic.entities.explode.RowBuffer`), and
        :meth:`write_statements` once it has packed its own. Tables arrive in
        :data:`~ftm_lakehouse.model.statement.SHARDED_SCHEMA` and go straight
        to :meth:`ParquetStore.append` – one is durable before the producer
        is asked for the next, which is what lets the journal drop a segment
        it has handed over. Sizing is the producer's call; each table becomes
        one parquet file per ``(shard, bucket, origin)`` partition it spans.

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
    def compact(self) -> None:
        """Bin-pack small parquet files within each partition."""
        self._statements.compact()

    @no_api
    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files tombstoned in the Delta log."""
        self._statements.vacuum(retention_hours=retention_hours)

    @no_api
    def export_statements_csv(self) -> None:
        """Export the statement store to the ``statements.csv`` artifact."""
        self._store.ensure_parent(self.EXPORTS_STATEMENTS)
        self._statements.export_csv(self.EXPORTS_STATEMENTS)

    @property
    def exists(self) -> bool:
        """Whether the statement store has been written."""
        return self._statements.exists

    @no_api
    def query_statements_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Query raw statement dicts from the parquet store.

        The fast local read: no :class:`~ftmq.store.lake.LakeStatement`
        construction – use :meth:`query_statements` for model objects.
        """
        yield from self._statements._query_statement_data(q)

    @no_api
    def unlock(self) -> bool:
        """Forcibly release the dataset write fence.

        Delegates to :meth:`ParquetStore.unlock`. Use as an operator
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
            :class:`~ftmq.store.lake.LakeStatement` objects.
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
            with (
                self._store.open(self.ENTITIES_JSON, "rb") as fh,
                decompress_stream(fh, self.compression) as raw,
            ):
                yield from smart_read_proxies(raw)

    @no_api
    def export_entities(self) -> None:
        """Export entities to a JSON lines file without FtM object construction.

        Uses :func:`aggregate_unsafe` to bypass Statement/StatementEntity/
        ``to_dict()`` and writes directly to orjson output.

        The statement source defaults to a **fresh** ``statements.csv`` when one
        exists – streaming the pre-sorted CSV into ``aggregate_unsafe`` is ~2x
        faster than re-scanning the parquet store, which pays per-row
        DuckDB→Python marshaling – and falls back to the live parquet view
        otherwise.

        Compression comes from :attr:`compression` (the dataset's config), not
        from the caller.
        """
        self._store.ensure_parent(self.ENTITIES_JSON)

        statements_csv_uri = self._fresh_statements_csv()
        if statements_csv_uri is not None:
            rows = self._stream_statements_csv(statements_csv_uri)
        else:
            rows = self._statements._query_statement_data()

        entities = aggregate_unsafe(rows, self.dataset)
        entities = (e.to_dict() for e in entities)

        with (
            self._store.open(self.ENTITIES_JSON, "wb") as fh,
            compress_stream(fh, self.compression) as out,
        ):
            smart_write_json(out, entities)

    def _stream_statements_csv(self, uri: str) -> Iterator[StatementDict]:
        """Stream the exported ``statements.csv`` as row dicts.

        Applies the dataset's codec on the way in: the CSV this reads is the
        artifact :meth:`ParquetStore.export_csv` just wrote, so on a
        compressed dataset it is a codec frame – and anystore's
        ``smart_stream_csv`` opens in text mode with no notion of
        compression. ``mode="r"`` asks for the text stream
        :class:`csv.DictReader` needs; an uncompressed dataset takes the same
        path, so there is no branch here.
        """
        with (
            # anystore types the handle as IO[Never] without a mode binding
            smart_open(uri, "rb") as fh,
            decompress_stream(cast(IO[bytes], fh), self.compression, "r") as raw,
        ):
            for row in csv.DictReader(raw):
                yield cast(StatementDict, row)

    def _fresh_statements_csv(self) -> str | None:
        """URI of the exported ``statements.csv`` if it's current, else ``None``.

        Current = its freshness tag (its own target key) is newer than
        ``statements/last_updated`` (bumped by appends *and* merges, so an
        optimize invalidates the CSV) and ``journal/last_updated`` (unflushed
        journal data means the CSV is behind). Within a full export run
        statements.csv is written first, so entity export streams it instead
        of re-scanning parquet.
        """
        if not self._store.exists(self.EXPORTS_STATEMENTS):
            return None
        deps = [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]
        if self._tags.is_latest(self.EXPORTS_STATEMENTS, deps):
            return self._store.to_uri(self.EXPORTS_STATEMENTS)
        return None

    def delete_entity(self, entity_id: str) -> int:
        """Delete all statements for an entity via journal tombstones.

        Reads statements from both parquet and journal, then UPSERTs
        tombstone rows (with deleted_at set) into the journal. Each
        tombstone carries the live row's ``fragment`` so it lands in the
        same supersession group – a bare tombstone would sit in the
        isolated non-fragment branch and never shadow a fragment row.

        Args:
            entity_id: The entity ID to delete

        Returns:
            Number of tombstone statements written
        """
        now = utc_now()
        stmts = self._collect_entity_statements(entity_id)
        if not stmts:
            return 0
        with self._journal.writer(self.shards) as w:
            for stmt in stmts:
                w.add_statement(stmt, deleted_at=now)
        self._tags.set(tag.JOURNAL_UPDATED)
        return len(stmts)

    def delete_statement(self, stmt: Statement, fragment: str | None = None) -> None:
        """Delete a single statement via journal tombstone.

        Args:
            stmt: The Statement to delete. A
                :class:`ftmq.store.lake.LakeStatement` (e.g. read back via
                :meth:`ParquetStore.get_statements`) carries its own
                fragment.
            fragment: Fragment override – required to shadow a
                fragment-bearing row when passing a plain ``Statement``;
                leave unset otherwise.
        """
        with self._tags.touch(tag.JOURNAL_UPDATED):
            now = utc_now()
            with self._journal.writer(self.shards) as w:
                w.add_statement(stmt, deleted_at=now, fragment=fragment)

    @no_api
    def _collect_entity_statements(self, entity_id: str) -> list[LakeStatement]:
        """Read all statements for an entity from parquet + journal.

        Uses shard-partitioned query for efficient single-entity lookup.
        Statements are keyed by :attr:`LakeStatement.dedupe_key` – the same
        statement content under distinct fragments is distinct for
        tombstoning purposes.
        """
        stmts_by_key: dict[str, LakeStatement] = {}

        q = Query(M(entity_id=entity_id))
        for stmt in self.query_statements(q):
            stmt = cast(LakeStatement, stmt)
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

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_ENTITIES

    @no_api
    def _get_changed_ids(self, since: datetime) -> Iterator[str]:
        """Get entity IDs with statements added since the given timestamp."""
        q = Query(C(first_seen__gte=since) | C(deleted_at__gte=since))
        return self._statements.get_entity_ids(q, source=self._statements.source_raw)

    @no_api
    def _write_diff(
        self, entity_ids: Iterator[str], since: datetime, ts: datetime
    ) -> str:
        """Write entities as line-based JSON with operation envelopes."""
        key = path.entities_diff(ts, self.compression)
        with (
            self._store.open(key, "wb") as o,
            compress_stream(o, self.compression) as out,
        ):
            smart_write_json(out, self._get_delta_entities(entity_ids, since))
        return self._store.to_uri(key)

    @no_api
    def _get_delta_entities(
        self, entity_ids: Iterator[str], since: datetime
    ) -> Generator[SDict, None, None]:
        """ADD envelopes for entities changed since ``since`` – one scoped
        subquery per partition via :meth:`ParquetStore.query_changed`, no
        per-batch ``IN`` loop – plus DEL envelopes for changed ids whose live
        statements are all gone."""
        original_ids: set[str] = set(entity_ids)
        seen_ids: set[str] = set()
        q = Query(C(first_seen__gte=since))
        for entity in self._statements._query_data(q):
            if entity.id:
                seen_ids.add(entity.id)
            yield make_envelope(entity.to_dict())
        for entity_id in original_ids - seen_ids:
            yield make_envelope({"id": entity_id}, op="DEL")

    @no_api
    def _write_initial_diff(self, ts: datetime, **kwargs) -> None:
        """Copy over exported entities.ftm.json to initial diff version.

        Both artifacts carry the dataset's codec, so the payload is decoded
        on the way in and re-encoded on the way out – the envelope is added
        per line, so this cannot be a byte copy.
        """
        with (
            self._store.open(self.ENTITIES_JSON, "rb") as i,
            decompress_stream(i, self.compression) as raw,
            self._store.open(path.entities_diff(ts, self.compression), "wb") as o,
            compress_stream(o, self.compression) as out,
        ):
            for data in raw:
                line = orjson.dumps(
                    make_envelope(orjson.loads(data)),
                    option=orjson.OPT_APPEND_NEWLINE,
                )
                out.write(line)
