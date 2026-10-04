"""ParquetStore – Delta Lake table with entity-hash shard partitioning.

Statements live in one Delta Lake table (per dataset) partitioned by
``(shard, bucket, origin)``. ``shard`` is the hex-padded entity_id hash bucket;
the uniform shard count is set per dataset via ``DatasetModel.shards``. It is
derived in [`ParquetStore.append`][ParquetStore.append] and nowhere else – producers hand over
``JOURNAL_SCHEMA`` rows with no shard key at all, so a partition can never be
picked against a shard count other than the one this store is configured for.

Writes are **append-only**: a flush lands each batch as new parquet files,
duplicates, re-emissions and tombstones included. Reads reconcile them: every
read runs over the files of one ``(shard, bucket)`` pair, taken from a Delta
snapshot the process keeps, and a partition holding files that `merge` did not
write is read through the dedupe query
([`dedupe_rows_sql`][ftm_lakehouse.logic.parquet.dedupe_rows_sql]) while a
partition made of merge output alone – canonical by construction – is a plain
``deleted_at IS NULL`` scan ([`live_rows_sql`][ftm_lakehouse.logic.parquet.live_rows_sql]).
So reads are correct at any time; `merge` is the compaction that makes them
cheap again and reaps tombstones past grace, `vacuum` drops the files a merge
replaced, and `shard` re-keys the whole store onto a different shard count –
the one operation that moves rows between partitions.

Statement-level reads iterate ``(shard, bucket)`` pairs and add ``WHERE shard =
? AND bucket = ?`` per query, keeping a full-store ``ORDER BY entity_id``
bounded to one partition; filters push through to the files' statistics.
``stats()`` and sorted / sliced queries go through the whole table.

Layout:
    statements/shard={s}/bucket={b}/origin={o}/part-*.parquet
"""

import json
import multiprocessing
import posixpath
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache, cached_property
from itertools import batched
from threading import RLock
from typing import Any, Callable, Iterable, Iterator, cast
from urllib.parse import unquote
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from anystore.decorators import error_handler
from anystore.interface.lock import Lock
from anystore.logging import get_logger
from anystore.logic.compress import CompressKind
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import Took, ensure_uuid, join_uri, mask_uri
from deltalake import DeltaTable, Schema, write_deltalake
from deltalake.exceptions import DeltaError, TableNotFoundError
from deltalake.transaction import AddAction, RemoveAction
from followthemoney.statement import StatementDict
from ftmq.model.stats import DatasetStats
from ftmq.query import Query, Sql, SqlSource
from ftmq.store.lake import (
    PRUNE,
    TARGET_SIZE,
    LakeStore,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from pyarrow.csv import CSVWriter  # type: ignore[attr-defined]  # missing from stubs
from rigour.time import utc_now
from sqlalchemy import Select

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.parquet import (
    MERGE_COMMIT_BATCH,
    MERGED_PREFIX,
    SWEEP_BATCH_SIZE,
    TABLE_CONFIGURATION,
    build_bounds_sample_sql,
    build_merge_sql,
    build_shard_sql,
    dedupe_rows_sql,
    delta_scan_sql,
    duckdb_config,
    live_rows_sql,
    live_view_sql,
    make_prune_by_shard,
    merge_copy_options,
    merge_duckdb_config,
    merge_slice_count,
    partition_source_sql,
    raw_view_sql,
    shard_target_file_size,
    slice_ranges,
)
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    TABLE_RAW,
    LakehouseStatement,
    statement_csv_select,
)
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.util import validate_origin

PARTITIONS = ["shard", "bucket", "origin"]

Partition = tuple[str, str, str]
"""A ``(shard, bucket, origin)`` partition key."""

Files = list[tuple[str, int]]
"""A partition's data files as ``(path, size)``: the path table-relative and
percent-decoded – the form DuckDB (prefixed with the table root) and a Delta
``add`` / ``remove`` action take; the log stores it encoded once more, and
``get_add_actions`` hands it back that way."""

Pairs = dict[tuple[str, str], list[tuple[Partition, Files, bool]]]
"""``(partition, files, clean)`` per ``(shard, bucket)`` pair, one entry per
origin partition. ``clean`` is whether every file was written by `merge`
(`MERGED_PREFIX`), i.e. whether a read over it can skip the dedupe."""


@cache
def make_source(table: str, shards: int) -> SqlSource:
    """Create `SqlSource` (live or raw) with configured shards"""
    config = {
        "id_column": "entity_id",
        "prune": {**PRUNE, "shard": make_prune_by_shard(shards)},
    }
    return SqlSource(table, **config)


class _LakeStore(LakeStore):
    """ftmq's store, with ``exists`` answered by the owning `ParquetStore`.

    ``LakeStore._execute`` – what ``stats()`` runs each of its aggregates
    through – checks ``exists`` per call, and ftmq answers that by loading a
    fresh ``DeltaTable``: one checkpoint replay per aggregate. The store's
    cached snapshot answers it for free.
    """

    def __init__(self, *args: Any, exists: Callable[[], bool], **kwargs: Any) -> None:
        self._exists = exists
        super().__init__(*args, **kwargs)

    @property
    def exists(self) -> bool:
        return self._exists()


@dataclass
class MergeTask:
    """One partition's merge as handed to a worker – plain data, so it pickles."""

    partition: Partition
    files: Files
    root: str
    grace_cutoff: datetime
    duckdb_config: dict[str, str]


@dataclass
class MergeResult:
    """What a worker wrote for one partition: ``(path, size, rows)`` per file."""

    files: list[tuple[str, int, int]]
    slices: int
    took: timedelta


def merge_partition(task: MergeTask) -> MergeResult:
    """Merge one partition into new data files next to its old ones.

    Writes but does not commit: the files are invisible until
    [`ParquetStore.merge`][ParquetStore.merge] commits them, so this runs in a
    worker process as well as in-process, and an uncommitted file is an orphan
    the next ``vacuum`` removes. Reads the partition's files directly
    ([`partition_source_sql`][ftm_lakehouse.logic.parquet.partition_source_sql]),
    so no Delta log is replayed here at all.

    A partition whose size suggests the merge would outgrow the worker's
    DuckDB memory limit is merged in ``entity_id`` range slices
    (``merge_slice_count``),
    one file per slice – ascending ranges, each sorted, so the partition's
    files together keep the file sort order. A slice left empty by the merge
    (everything in it reaped) gets no ``add`` action.
    """
    shard, bucket, origin = task.partition
    directory = posixpath.dirname(task.files[0][0])
    size = sum(s for _, s in task.files)
    config: dict[str, Any] = {**task.duckdb_config}
    with Took() as t, closing(duckdb.connect(config=config)) as con:
        files = [f"{task.root}/{file}" for file, _ in task.files]
        source = partition_source_sql(files, *task.partition)
        ranges: list[tuple[str | None, str | None]] = [(None, None)]
        slices = merge_slice_count(size, task.duckdb_config["memory_limit"])
        if slices > 1:
            sample_sql = build_bounds_sample_sql(shard, bucket, origin, source=source)
            sample = [r[0] for r in con.execute(sample_sql).fetchall()]
            ranges = slice_ranges(sample, slices)
        written: list[tuple[str, int, int]] = []
        for entity_range in ranges:
            sql = build_merge_sql(
                shard,
                bucket,
                origin,
                task.grace_cutoff,
                entity_id_range=entity_range,
                source=source,
                select=f"* EXCLUDE ({', '.join(PARTITIONS)})",
            )
            file = f"{directory}/{MERGED_PREFIX}{ensure_uuid()}.zstd.parquet"
            target = f"{task.root}/{file}".replace("'", "''")
            # RETURN_STATS: (filename, count, file_size_bytes, ...)
            stats = con.execute(
                f"COPY ({sql}) TO '{target}' ({merge_copy_options(bucket)})"
            ).fetchone()
            if stats and stats[1]:
                written.append((file, int(stats[2]), int(stats[1])))
    return MergeResult(files=written, slices=len(ranges), took=t.took)


class ParquetStore:
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: [`append`][ParquetStore.append] writes each batch as
    new parquet files. Reads reconcile whatever the files hold – duplicates,
    superseded fragments, tombstones – unless a partition is made of
    [`merge`][ParquetStore.merge] output alone, which is canonical and read as
    a plain scan. [`merge`][ParquetStore.merge], [`compact`][ParquetStore.compact]
    and [`vacuum`][ParquetStore.vacuum] are therefore maintenance, not a
    precondition for correct reads.
    """

    def __init__(
        self,
        uri: Uri,
        dataset: str,
        shards: int | None = None,
        compression: CompressKind | None = None,
    ) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        self.settings = Settings()
        self.dataset = dataset
        self.shards = shards if shards is not None else DEFAULT_SHARDS
        # Resolved from the dataset config (`DatasetHandle._model`) by the
        # owning repository – exports never take a runtime codec.
        self.compression = compression
        self._store = get_store(uri)
        self._tags = TagStore(uri)
        self._snapshot_lock = RLock()
        self._snapshot: DeltaTable | None = None
        self._pairs: tuple[int, Pairs] | None = None
        self._lake = _LakeStore(
            uri=str(self.uri),
            dataset=self.dataset,
            partition_by=PARTITIONS,
            view_sqls={
                TABLE.name: live_view_sql,
                TABLE_RAW.name: raw_view_sql,
            },
            duckdb_config=duckdb_config(),
            exists=lambda: self.exists,
        )
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            uri=mask_uri(self.uri),
        )

    @property
    def deltatable(self) -> DeltaTable:
        """A freshly loaded handle on the table – for callers outside the
        store. Everything in here shares `_current_snapshot`."""
        return self._lake.deltatable

    @property
    def num_rows(self) -> int:
        """Physical row count from the snapshot's file statistics – no scan."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            return snapshot.count() if snapshot is not None else 0

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            return snapshot.version() if snapshot is not None else None

    @property
    def exists(self) -> bool:
        """Check existence of deltatable"""
        with self._snapshot_lock:
            return self._current_snapshot() is not None

    def _current_snapshot(self) -> DeltaTable | None:
        """This process's Delta snapshot, brought up to date – ``None``
        without a table.

        Loaded once, then advanced with ``update_incremental``, which reads only
        the commits since. Loading a ``DeltaTable`` replays the latest
        checkpoint – the whole file list of the table – and every read and
        append used to do that, some several times. A snapshot whose next
        commits the log retention already deleted is loaded afresh.

        Callers hold `_snapshot_lock`: the object is shared by this process's
        threads, and an append writes through it.
        """
        if self._snapshot is not None:
            try:
                self._snapshot.update_incremental()
                return self._snapshot
            except DeltaError:
                self._snapshot = None
        try:
            self._snapshot = DeltaTable(
                str(self.uri), storage_options=storage_options()
            )
        except TableNotFoundError:
            return None
        return self._snapshot

    def _snapshot_pairs(self) -> tuple[str, Pairs]:
        """The table root and the current snapshot's data files per
        ``(shard, bucket)`` pair, from its add actions – file-level metadata,
        no data scan. Regrouped only when the version moved; empty without a
        table."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return "", {}
            version = snapshot.version()
            if self._pairs is None or self._pairs[0] != version:
                actions = pa.table(snapshot.get_add_actions(flatten=True))
                partitions: dict[Partition, Files] = {}
                for file, size, shard, bucket, origin in zip(
                    actions["path"].to_pylist(),
                    actions["size_bytes"].to_pylist(),
                    actions["partition.shard"].to_pylist(),
                    actions["partition.bucket"].to_pylist(),
                    actions["partition.origin"].to_pylist(),
                ):
                    key = (shard, bucket, origin)
                    partitions.setdefault(key, []).append((unquote(file), size))
                pairs: Pairs = {}
                for partition, files in sorted(partitions.items()):
                    clean = all(
                        posixpath.basename(file).startswith(MERGED_PREFIX)
                        for file, _ in files
                    )
                    pairs.setdefault(partition[:2], []).append(
                        (partition, files, clean)
                    )
                self._pairs = (version, pairs)
            return snapshot.table_uri.rstrip("/"), self._pairs[1]

    def _snapshot_partitions(self) -> tuple[str, dict[Partition, tuple[Files, bool]]]:
        """The table root and the snapshot's ``(files, clean)`` per partition."""
        root, pairs = self._snapshot_pairs()
        return root, {
            partition: (files, clean)
            for sources in pairs.values()
            for partition, files, clean in sources
        }

    def _dirty_partitions(self) -> dict[Partition, Files]:
        """The partitions holding a file `merge` did not write, with their
        files – what a default merge rewrites."""
        _, partitions = self._snapshot_partitions()
        return {p: files for p, (files, clean) in partitions.items() if not clean}

    @cached_property
    def source(self) -> SqlSource:
        return make_source(TABLE, self.shards)

    @cached_property
    def source_raw(self) -> SqlSource:
        return make_source(TABLE_RAW, self.shards)

    def _compile_query(self, q: Query | None = None) -> Select:
        """Compile ``q`` to a statements ``Select`` against the live view.

        Compiles through `self.source`, so a schema filter folds into
        a ``bucket IN (...)`` predicate (ftmq's `SqlSource`
        ``prune``) and a schema-scoped read prunes to the matching bucket
        partitions instead of scanning all of them. The single entry point every
        lakehouse read funnels its `Query` through.
        """
        return (q or Query()).compile(self.source)

    def _statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Statement dicts for ``q``.

        A sorted or sliced query runs as ONE query over the whole table: the
        compiled ``LIMIT`` / ``OFFSET`` live in ftmq's un-scoped
        ``canonical_ids`` subquery and ``ORDER BY`` only orders within a
        partition, so under the per-``(shard, bucket)`` iteration it would
        over-return (one limit *per partition*) and mis-order. It reads
        ``delta_scan`` on a cursor of its own (`_cursor_over`), reconciling
        unless every pair it can touch is clean. Everything else iterates the
        pairs (`_query_statement_data`). Rows stay entity-contiguous either
        way – ftmq's statement selects order by ``entity_id`` (unsorted) or
        ``(sortable_value, id)`` (sorted) – so aggregation can run over the
        stream directly.
        """
        if q is not None and (q.sort is not None or q.slice is not None):
            root, pairs = self._snapshot_pairs()
            if not root:
                return
            keys = self._pruned_keys(pairs, self._prune_values(q, self.source))
            clean = all(c for key in keys for _, _, c in pairs[key])
            compiled = str(
                self._compile_query(q).compile(compile_kwargs={"literal_binds": True})
            )
            with self._cursor_over(delta_scan_sql(root), clean) as cur:
                res = cur.execute(compiled)
                columns = [d[0] for d in res.description]
                while rows := res.fetchmany(100_000):
                    for row in rows:
                        yield cast(StatementDict, dict(zip(columns, row)))
        else:
            yield from self._query_statement_data(q)

    def query(self, q: Query | None = None) -> StatementEntities:
        """Query entities from the store.

        Args:
            q: Optional ``Query`` of entity-level filters (schema, properties,
                ids, ...) plus ordering / slicing – a sorted or sliced query
                executes globally (`_statement_data`) so ``LIMIT`` and
                ``ORDER BY`` hold across partitions.

        Yields:
            StatementEntity objects matching the query.
        """
        for data in self._query_data(q):
            yield data.to_entity()

    def query_statements(self, q: Query | None = None) -> Statements:
        """Query ordered Statements from the store.

        Args:
            q: Optional ``Query`` – executed via `_statement_data`;
                sorted / sliced queries execute globally.

        Yields:
            `LakehouseStatement` objects matching the query – carrying their
            ``fragment`` and ``role``, so a statement read back here can be
            handed straight to
            [`delete_statement`][ftm_lakehouse.repository.EntityRepository.delete_statement]
            and land in the merge group it came from.
        """
        for stmt_dict in self._statement_data(q):
            yield LakehouseStatement.from_dict(stmt_dict)

    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store.

        Runs ftmq's aggregation SQL over the connection-level ``statement``
        view ([`live_view_sql`][ftm_lakehouse.logic.parquet.live_view_sql]),
        which always reconciles – correct on any store, cheapest on a merged
        one.
        """
        return self._lake.default_view().stats()

    def _write_lock(self) -> Lock:
        """Exclusive side of the dataset write fence.

        Held by maintenance ([`merge`][ParquetStore.merge],
        [`compact`][ParquetStore.compact], [`vacuum`][ParquetStore.vacuum] via
        `_maintenance_fence`) and by the first-ever
        [`append`][ParquetStore.append] of a dataset (table creation must not
        race). The lock lives at
        ``{dataset_root}/.LOCK`` per ``path.LOCK``.

        Regular appends do **not** take this lock – they register a shared
        marker instead (`_append_fence`); Delta's optimistic
        concurrency serializes concurrent append commits safely on its own.

        Acquisition is bounded by ``settings.lock_max_retries`` (total wait
        roughly ``N²/2`` seconds); entering the returned lock raises
        ``RuntimeError`` when the fence stays busy, so contended writers fail
        instead of pinning a thread forever. A lock left behind by a crashed
        writer must be released manually via [`unlock`][ParquetStore.unlock]
        (``ftm-lakehouse maintenance unlock``).
        """
        return Lock(
            self._store, key=path.LOCK, max_retries=self.settings.lock_max_retries
        )

    def _fence_retry(self, attempt: Callable[[], None]) -> None:
        """Retry ``attempt`` until it stops raising, with the fence's bound.

        The retry policy is anystore's ``error_handler`` with
        ``backoff_factor=1`` – the same engine ``Lock`` acquisition composes:
        attempt ``N`` sleeps ``N`` seconds plus up to one second of jitter
        (so concurrent waiters don't wake in lockstep), and
        ``settings.lock_max_retries`` attempts wait roughly ``N²/2`` seconds
        in total before the ``RuntimeError`` propagates (``do_raise=True`` –
        without it a still-busy fence would silently pass).
        """
        error_handler(
            max_retries=self.settings.lock_max_retries,
            backoff_factor=1,
            do_raise=True,
        )(attempt)()

    def _await(self, ready: Callable[[], bool], what: str) -> None:
        """Block until ``ready()`` is true, with the fence's retry bound."""

        def check() -> None:
            if not ready():
                raise RuntimeError(
                    f"Write fence busy: {what}. If a writer crashed, release "
                    "the fence via `ftm-lakehouse maintenance unlock`."
                )

        self._fence_retry(check)

    def _append_markers(self) -> list[str]:
        """Keys of all currently registered append markers."""
        return list(self._store.iterate_keys(prefix=str(path.LOCK_APPENDS)))

    @contextmanager
    def _append_fence(self) -> Iterator[None]:
        """Shared (append) side of the dataset write fence.

        Registers a marker key under ``.LOCK-APPENDS/`` and only *then*
        checks the maintenance ``.LOCK`` – the store-then-load order makes
        the handshake sound on a linearizable store: when the ``.LOCK``
        check sees no lock, the marker write is already visible to any
        later drain poll by a maintenance holder, so
        `_maintenance_fence` can never pass its drain while an
        unnoticed append is in flight. When ``.LOCK`` is held, the marker
        is removed *before* backing off (a parked appender must not
        deadlock the drain), then register-and-check retries under the
        fence's usual bound.

        Concurrent appends never block each other – Delta append commits
        are blind appends that delta-rs serializes via optimistic commit
        retries. A marker left behind by a crashed appender blocks
        maintenance until released via [`unlock`][ParquetStore.unlock]
        (``ftm-lakehouse maintenance unlock``).
        """
        marker = f"{path.LOCK_APPENDS}/{uuid4().hex}"

        def register() -> None:
            self._store.touch(marker)
            if self._store.exists(path.LOCK):
                self._store.delete(marker, ignore_errors=True)
                raise RuntimeError(
                    f"Write fence busy: maintenance lock `{path.LOCK}` is "
                    "held. If a writer crashed, release the fence via "
                    "`ftm-lakehouse maintenance unlock`."
                )

        self._fence_retry(register)
        try:
            yield
        finally:
            self._store.delete(marker, ignore_errors=True)

    def _ensure_table(self) -> None:
        """Create the Delta table (as an empty commit) if it does not exist.

        Runs under the exclusive write lock so two racing first imports
        cannot both commit version ``0``. Establishing existence here –
        once, at the first write – lets [`append`][ParquetStore.append] always take the
        shared append fence with ``mode="append"`` instead of
        special-casing creation inside the hot write path.
        """
        if self.exists:
            return
        with self._write_lock():
            if self.exists:  # lost the create race - the table is there now
                return
            write_deltalake(
                str(self.uri),
                pa.Table.from_pylist([], schema=SHARDED_SCHEMA),
                partition_by=PARTITIONS,
                mode="overwrite",
                configuration=TABLE_CONFIGURATION,
                storage_options=storage_options(),
            )

    @contextmanager
    def _maintenance_fence(self) -> Iterator[None]:
        """Exclusive fence for partition-rewriting maintenance.

        Acquires the ``.LOCK`` write lock (fencing off other maintenance and
        new appends), then waits for in-flight append markers to drain so a
        rewrite never overlaps an append it could tombstone.
        """
        with self._write_lock():
            self._await(
                lambda: not self._append_markers(),
                f"append markers under `{path.LOCK_APPENDS}/` are present",
            )
            yield

    def unlock(self) -> bool:
        """Forcibly release the dataset write fence.

        Operator escape hatch for the case where a writer process died
        with the fence held (or an attacker held it on purpose). Releases
        both sides: the exclusive ``.LOCK`` file and any append markers
        under ``.LOCK-APPENDS/``.

        **Use sparingly** – breaking a fence that's still held by a live
        writer can corrupt a write in flight. Confirm no process is
        actively writing before running.

        Returns:
            ``True`` if a lock or marker was released, ``False`` if the
            fence was clear.
        """
        released = False
        if self._store.exists(path.LOCK):
            self._store.delete(path.LOCK)
            released = True
        for marker in self._append_markers():
            self._store.delete(marker, ignore_errors=True)
            released = True
        return released

    def evolve_schema(self) -> list[str]:
        """Add the `SHARDED_SCHEMA` columns this table was created without.

        Metadata-only Delta schema evolution – one commit against the table's
        schema, no parquet file rewritten: ``delta_scan`` reads a column the
        older files don't carry as NULL, which is the "absent" sentinel of
        every nullable column anyway, so nothing is owed a re-merge.

        Additive only – Delta has no metadata-only drop without column mapping,
        so a column *removed* from `SHARDED_SCHEMA` needs a full rewrite
        instead. Idempotent, and held under the exclusive maintenance fence.

        Returns:
            Names of the columns added – empty if the table is already current
            or does not exist yet.
        """
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return []
            known = {f.name for f in snapshot.schema().fields}
            missing = [
                f
                for f in Schema.from_arrow(SHARDED_SCHEMA).fields
                if f.name not in known
            ]
            if not missing:
                return []
            names = [f.name for f in missing]
            with self._maintenance_fence():
                snapshot.alter.add_columns(missing)
        self.log.info("Evolved parquet schema.", columns=names)
        return names

    def configure_table(self) -> dict[str, str]:
        """Apply `TABLE_CONFIGURATION` to a table created without it.

        Sets the properties, then writes a checkpoint under them – which drops
        the ``remove`` actions past the new retention, so every reader from
        here on replays a smaller one – and deletes the log entries and
        checkpoints past the new log retention. Idempotent, and held under the
        exclusive maintenance fence: a metadata commit racing an append would
        fail it.

        Returns:
            The properties that changed – empty if the table is already
            configured or does not exist yet.
        """
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return {}
            current = snapshot.metadata().configuration
            changed = {
                k: v for k, v in TABLE_CONFIGURATION.items() if current.get(k) != v
            }
            if not changed:
                return {}
            with self._maintenance_fence(), Took() as t:
                snapshot.alter.set_table_properties(changed)
                snapshot.update_incremental()
                snapshot.create_checkpoint()
                snapshot.cleanup_metadata()
        self.log.info("Configured delta table.", took=t.took, **changed)
        return changed

    def _with_shard(self, batch: pa.Table) -> pa.Table:
        """Derive the ``shard`` partition key from ``entity_id``.

        The single point where a row's partition is decided, so ``shard`` is
        always a function of ``entity_id`` and *this* store's configured
        count – never of what some producer computed earlier, possibly
        against a different config. That is what keeps a stale writer from
        mis-routing rows: the journal carries no shard key
        (`JOURNAL_SCHEMA`), so there is
        nothing stale to trust.

        Hashes the *distinct* entity ids rather than every row – statements
        come many per entity, so the dictionary detour costs a fraction of a
        row-wise loop and the ``take`` is vectorized.
        """
        ids = pc.dictionary_encode(batch.column("entity_id").combine_chunks())
        shards = pa.array(
            [entity_shard(e, self.shards) for e in ids.dictionary.to_pylist()],
            pa.string(),
        )
        return batch.append_column(
            SHARDED_SCHEMA.field("shard"), pc.take(shards, ids.indices)
        ).select(SHARDED_SCHEMA.names)

    def append(self, batch: pa.Table) -> None:
        """Append a batch of statements.

        Rows arrive in
        `JOURNAL_SCHEMA` – without a
        ``shard`` column – and `_with_shard` derives it here. Batches
        may span any number of shards; each one becomes a parquet file per
        ``(shard, bucket, origin)`` partition it touches, so a bigger batch
        costs fewer files, not more. The method splits by ``bucket`` so each
        ``write_deltalake`` call uses the bucket-appropriate
        ``writer_properties`` (small vs. large profile). Duplicates land as
        separate rows and are reaped by [`merge`][ParquetStore.merge].

        Deliberately does **not** sort. Nothing downstream reads in physical
        order, and [`merge`][ParquetStore.merge] rewrites every partition an append touched
        into the file sort order anyway.

        Held under the *shared* side of the write fence
        (`_append_fence`): concurrent appends run in parallel – Delta
        serializes their commits via optimistic concurrency – while
        [`merge`][ParquetStore.merge] / [`compact`][ParquetStore.compact] /
        [`vacuum`][ParquetStore.vacuum] wait for the append markers to drain
        before rewriting partitions. Table creation happens
        once in `_ensure_table` (under the exclusive lock, so two
        racing imports can't both commit version ``0``); the write loop
        itself always appends. The files delta-rs writes are named
        ``part-*``, which is what marks their partitions dirty for the next
        [`merge`][ParquetStore.merge] (`MERGED_PREFIX`) – no tag to stamp.

        Writes through this process's snapshot (`_current_snapshot`),
        advanced to the latest commit first, instead of loading the table per
        write – a load replays the whole file list, which made an append's cost
        grow with the store. Appends of one process are serialised on the
        snapshot; appends of different processes still commit concurrently.

        Args:
            batch: PyArrow table with the columns of
                `JOURNAL_SCHEMA`.
        """
        if len(batch) == 0:
            return

        batch = self._with_shard(batch)
        buckets = pc.unique(batch["bucket"]).to_pylist()
        shards = pc.unique(batch["shard"]).to_pylist()
        self.log.info(
            f"Flushing {len(batch)} statements to parquet ...",
            buckets=buckets,
            shards=shards,
        )
        with self._tags.touch(tag.STATEMENTS_UPDATED):
            self._ensure_table()
            with self._append_fence():
                with self._snapshot_lock:
                    snapshot = self._current_snapshot()
                    if snapshot is None:
                        raise RuntimeError(f"Statement store vanished: `{self.uri}`")
                    for bucket in buckets:
                        sub = batch.filter(pc.equal(batch["bucket"], bucket))
                        write_deltalake(
                            snapshot,
                            sub,
                            partition_by=PARTITIONS,
                            mode="append",
                            writer_properties=writer_for_bucket(bucket),
                        )

    @property
    def needs_merge(self) -> bool:
        """Whether any partition holds a file [`merge`][ParquetStore.merge] did
        not write – and so reads through the dedupe query until it does.

        Answered from the snapshot's file list (`MERGED_PREFIX`), which is
        what `merge` itself selects partitions by, so the two cannot disagree.
        """
        return bool(self._dirty_partitions())

    def merge(self, force: bool = False) -> None:
        """Collapse duplicates and reap expired tombstones, partition by partition.

        For each ``(shard, bucket, origin)`` partition, runs the merge
        query ([`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql] –
        non-fragment rows: keep latest row per ``id`` by ``last_seen DESC``;
        fragment rows: keep the latest emission per ``(entity_id, prop,
        fragment)`` group; fold ``first_seen`` to the min; drop tombstones older
        than the grace cutoff) and replaces the partition's files with the
        result. Held under the exclusive maintenance fence (``path.LOCK`` +
        append-marker drain, `_maintenance_fence`).

        Only dirty partitions are rewritten – those holding a file this
        method did not write (`MERGED_PREFIX`), i.e. appended since their
        last merge – so an optimize after a small append rewrites only what
        changed instead of the whole store. The signal is the snapshot's
        file list; nothing is stamped.

        Because a clean partition is never revisited by a *default* merge,
        a tombstone sitting in an otherwise-idle partition is not
        physically reaped once it passes the grace window until the next
        write touches that partition – this only defers disk reclamation;
        read correctness is unaffected (the live view hides tombstones
        regardless). ``force=True`` bypasses the skip and re-evaluates
        every partition, so a forced merge (with
        ``LAKEHOUSE_GRACE_PERIOD_DAYS=0`` for an immediate purge)
        physically reaps cold tombstones too.

        An optimisation, not a precondition: reads reconcile a dirty
        partition with the same dedupe query this writes, so the rows a read
        returns are the same before and after. What changes is the cost – a
        clean partition is a plain scan – and the disk, once tombstones past
        grace and the rows they shadow are gone. The commit says so in Delta's
        own terms: its ``add`` and ``remove`` actions carry ``dataChange =
        false``, the mark of a rewrite that changes no logical content.

        The Delta log is read once per run, not once per partition. On a store
        whose log has grown large, replaying it is what a merge spent its time
        on: every ``delta_scan`` and every ``write_deltalake`` replays the
        latest checkpoint, which holds every live file of the table. So the
        run loads one snapshot, hands each partition's files from it to
        [`merge_partition`][merge_partition] – which reads them directly and
        writes the merged files with DuckDB – and commits the results in
        batches of `MERGE_COMMIT_BATCH` partitions, each batch one Delta
        transaction of ``add`` and ``remove`` actions against that snapshot.
        A batch is atomic: its partitions switch to their merged files
        together, or not at all.

        Partitions merge in ``LAKEHOUSE_MERGE_WORKERS`` processes (one, the
        default, merges in this process). They are independent – a
        worker writes files and commits nothing – so they parallelise
        without coordination; the DuckDB memory limit and the threads are
        split between the workers
        ([`merge_duckdb_config`][ftm_lakehouse.logic.parquet.merge_duckdb_config]).

        Args:
            force: Rewrite every partition, clean ones included – with
                ``LAKEHOUSE_GRACE_PERIOD_DAYS=0`` that purges cold tombstones.
        """
        if not self.exists:
            return
        grace_cutoff = utc_now() - timedelta(days=self.settings.grace_period_days)
        workers = max(self.settings.merge_workers, 1)
        config = merge_duckdb_config(workers)
        merged = skipped = 0
        with self._maintenance_fence():
            # appends are fenced off from here on, so no partition can be
            # written after this and still read as merged
            root, partitions = self._snapshot_partitions()
            tasks: list[MergeTask] = []
            for partition, (files, clean) in partitions.items():
                if clean and not force:
                    skipped += 1
                    continue
                tasks.append(MergeTask(partition, files, root, grace_cutoff, config))
            with self._merge_runner(workers) as run:
                results = zip(tasks, run(merge_partition, tasks))
                for batch in batched(results, MERGE_COMMIT_BATCH):
                    self._commit_merged(batch)
                    merged += len(batch)
        self.log.info(
            "Merge complete.",
            merged=merged,
            skipped=skipped,
            workers=workers,
            grace_period_days=self.settings.grace_period_days,
        )

    @staticmethod
    @contextmanager
    def _merge_runner(workers: int) -> Iterator[Callable[..., Iterator[Any]]]:
        """An ordered ``map`` over ``workers`` processes – the builtin for one.

        Processes are spawned rather than forked: this process holds a DuckDB
        instance with its own threads, which a fork would copy mid-flight.
        Pending tasks are cancelled when the run fails, instead of merging
        partitions whose results nobody will commit.
        """
        if workers == 1:
            yield map
            return
        context = multiprocessing.get_context("spawn")
        pool = ProcessPoolExecutor(workers, mp_context=context)
        try:
            yield pool.map
        finally:
            pool.shutdown(cancel_futures=True)

    def _commit_merged(self, batch: Iterable[tuple[MergeTask, MergeResult]]) -> None:
        """Commit a batch of merged partitions as one Delta transaction.

        Each partition's merged files are added and every file it was merged
        from removed (`Files` paths – ``create_write_transaction`` encodes them
        for the log), all with ``dataChange = false``: the merge changes no
        logical content. Committed through the process's snapshot, advanced
        past the commit, so the next batch is not checked against a version
        this one already superseded.
        """
        batch = list(batch)
        now = int(utc_now().timestamp() * 1000)
        actions: list[AddAction | RemoveAction] = []
        for task, result in batch:
            values: dict[str, str | None] = dict(zip(PARTITIONS, task.partition))
            for file, size, rows in result.files:
                stats = json.dumps({"numRecords": rows})
                actions.append(AddAction(file, size, values, now, False, stats))
            for file, size in task.files:
                actions.append(RemoveAction(file, False, now, size, values))
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                raise RuntimeError(f"Statement store vanished: `{self.uri}`")
            snapshot.create_write_transaction(
                actions,
                mode="append",
                schema=snapshot.schema(),
                partition_by=PARTITIONS,
            )
            snapshot.update_incremental()
        for task, result in batch:
            shard, bucket, origin = task.partition
            self.log.info(
                f"Merged partition `{shard}/{bucket}/{origin}`.",
                took=result.took,
                shard=shard,
                bucket=bucket,
                origin=origin,
                grace_period_days=self.settings.grace_period_days,
                slices=result.slices,
            )

    def _chained_reader(
        self, cur: duckdb.DuckDBPyConnection, sqls: list[str]
    ) -> pa.RecordBatchReader:
        """Chain queries into one lazily-executed reader for a single write.

        Each query's ``to_arrow_reader`` streams from DuckDB's execution
        pipeline and ``write_deltalake`` consumes batch by batch, so a
        rewrite never materialises its input in Python memory. The
        queries execute strictly sequentially – query ``i + 1`` only
        starts once query ``i`` is exhausted – so at most one of them
        holds a sort window in DuckDB at a time.

        [`shard`][ParquetStore.shard] feeds it one query per source
        partition, deliberately unordered.

        Args:
            cur: Open DuckDB cursor – must stay alive until the returned
                reader is fully consumed.
            sqls: Queries in output order.
        """
        first = cur.execute(sqls[0]).to_arrow_reader()

        def batches() -> Iterator[pa.RecordBatch]:
            yield from first
            for sql in sqls[1:]:
                yield from cur.execute(sql).to_arrow_reader()

        return pa.RecordBatchReader.from_batches(first.schema, batches())

    def shard(self, shards: int) -> None:
        """Re-key the whole store onto ``shards`` entity-hash shards.

        The physical half of a shard-count change: every row's ``shard``
        is recomputed from its ``entity_id``
        ([`build_shard_sql`][ftm_lakehouse.logic.parquet.build_shard_sql]) and the
        store is rewritten into the new partition layout. ``bucket`` and
        ``origin`` are invariant under re-sharding – only ``shard``
        moves – so the rewrite runs one ``write_deltalake`` per
        ``(bucket, origin)`` group, replacing that group's partitions
        wholesale via ``predicate`` while the group's source partitions
        stream in through a single chained reader
        (`_chained_reader`). Nothing is materialised in Python, and
        each group's rows land in one atomic Delta commit with the
        bucket-appropriate ``writer_properties``.

        One writer per *target* partition stays open across a group's
        write, so the target file size is scaled down by the shard count
        (`shard_target_file_size`) to
        keep their combined buffers bounded; the resulting small files are
        what the follow-up ``compact`` bin-packs.

        Deliberately no dedupe and no sort: the use case is a store whose
        queries have outgrown their shard count, and a re-shard moves
        rows rather than deciding which survive. Every rewritten partition
        comes out dirty – delta-rs names its files ``part-*``, not
        `MERGED_PREFIX` – so reads reconcile it and the next
        [`merge`][ParquetStore.merge] restores the file sort order; run
        ``optimize`` afterwards. The dataset-level clocks stay put, because a
        re-shard changes physical layout, not content, and the exports keyed
        on them are byte-identical either side of it.

        Idempotent: the target shard is a function of ``entity_id`` and
        the target count alone, never of the value a row currently
        carries, so a run interrupted between group commits is repaired
        by running it again.

        Held under the exclusive maintenance fence
        (`_maintenance_fence`), which blocks parquet appends but
        **not** journal writes. Journalled rows carry no shard key, so a
        flush *after* this returns places them under the new count – but
        one landing between the rewrite and the config write still resolves
        the old one. Run with writers stopped.

        Args:
            shards: Target shard count; ``<= 1`` collapses the store into
                the single ``"0"`` shard.
        """
        if self.exists:
            self._rewrite_shards(shards)
        self.shards = shards
        # the cached sources prune by the shard count they were built with
        self.__dict__.pop("source", None)
        self.__dict__.pop("source_raw", None)
        self.log.info("Re-shard complete.", shards=shards)

    def _rewrite_shards(self, shards: int) -> None:
        """Rewrite every ``(bucket, origin)`` group onto ``shards`` shards.

        Reads the Delta log once, as [`merge`][ParquetStore.merge] does: the
        source partitions' files come from the process's snapshot
        ([`partition_source_sql`][ftm_lakehouse.logic.parquet.partition_source_sql]),
        and every group write goes through it, advanced past each commit. A
        ``delta_scan`` per source partition and a reload per write replayed
        the whole log once per partition of the store. The snapshot lock is
        held for each group's write – a re-shard runs with writers stopped,
        from a process that does nothing else.
        """
        with self._maintenance_fence():
            root, partitions = self._snapshot_partitions()
            groups: dict[tuple[str, str], list[tuple[Partition, Files]]] = {}
            for partition, (files, _) in partitions.items():
                _, bucket, origin = partition
                groups.setdefault((bucket, origin), []).append((partition, files))
            config: dict[str, Any] = {**duckdb_config()}
            for (bucket, origin), sources in groups.items():
                with (
                    Took() as t,
                    closing(duckdb.connect(config=config)) as con,
                    self._snapshot_lock,
                ):
                    snapshot = self._current_snapshot()
                    if snapshot is None:
                        raise RuntimeError(f"Statement store vanished: `{self.uri}`")
                    sqls = [
                        build_shard_sql(
                            *partition,
                            shards,
                            source=partition_source_sql(
                                [f"{root}/{file}" for file, _ in files], *partition
                            ),
                        )
                        for partition, files in sources
                    ]
                    write_deltalake(
                        snapshot,
                        self._chained_reader(con, sqls),
                        mode="overwrite",
                        partition_by=PARTITIONS,
                        predicate=(
                            f"bucket = '{bucket}' AND "
                            f"origin = '{validate_origin(origin)}'"
                        ),
                        writer_properties=writer_for_bucket(bucket),
                        target_file_size=shard_target_file_size(shards),
                    )
                    snapshot.update_incremental()
                self.log.info(
                    f"Re-sharded `{bucket}/{origin}`.",
                    took=t.took,
                    bucket=bucket,
                    origin=origin,
                    sources=len(sources),
                    shards=shards,
                )

    def delete_origin(self, origin: str) -> int:
        """Physically drop every row of one origin.

        ``origin`` is a partition column, so the predicate prunes to whole
        partitions and Delta drops their files instead of rewriting rows –
        unlike [`merge`][ParquetStore.merge]'s tombstone reap this is
        immediate, with no grace period and nothing left to collapse. Held
        under the exclusive maintenance fence
        (`_maintenance_fence`), like the other partition-level
        rewrites.

        Stamps
        [`STATEMENTS_UPDATED`][ftm_lakehouse.core.conventions.tag.STATEMENTS_UPDATED]
        on completion when rows were removed: the store's content moved, so
        exports, statistics and diffs have to go stale against it.

        Args:
            origin: The origin tag to drop.

        Returns:
            Number of rows removed.

        Raises:
            ValueError: If ``origin`` is not a safe origin name
                (see `validate_origin`).
            RuntimeError: When the write fence cannot be acquired.
        """
        origin = validate_origin(origin)
        if not self.exists:
            return 0
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return 0
            # safe to interpolate: `validate_origin` rejects quotes
            metrics = snapshot.delete(f"origin = '{origin}'")
            deleted = int(metrics.get("num_deleted_rows") or 0)
            if deleted:
                self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Dropped origin.",
                took=t.took,
                origin=origin,
                deleted=deleted,
                **metrics,
            )
        return deleted

    def compact(self) -> None:
        """Bin-pack small parquet files within each partition.

        Cheap maintenance – Delta's ``OPTIMIZE compact`` only rewrites small
        files into larger ones; it does not collapse duplicate rows or drop
        tombstones (use [`merge`][ParquetStore.merge] for that). Held under the exclusive
        maintenance fence (`_maintenance_fence`).
        """
        if not self.exists:
            return
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is not None:
                for shard, bucket, origin in self._list_partitions():
                    snapshot.optimize.compact(
                        partition_filters=[
                            ("shard", "=", shard),
                            ("bucket", "=", bucket),
                            ("origin", "=", origin),
                        ],
                        writer_properties=writer_for_bucket(bucket),
                        target_size=TARGET_SIZE,
                    )
        self.log.info("Compaction done.", took=t.took)

    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files no longer referenced by the Delta log.

        Tombstoned files (replaced by [`merge`][ParquetStore.merge] /
        [`compact`][ParquetStore.compact]) become orphans on disk; vacuum
        prunes them once they're past
        ``retention_hours``. Held under the exclusive maintenance fence
        (`_maintenance_fence`).

        Args:
            retention_hours: Keep files newer than this many hours. ``0``
                drops every file the Delta log no longer references.
        """
        if not self.exists:
            return
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is not None:
                snapshot.vacuum(
                    retention_hours=retention_hours,
                    dry_run=False,
                    enforce_retention_duration=False,
                )
        self.log.info("Vacuumed.", took=t.took)

    def sweep(
        self, csv_key: str | None = None, tee: bool = True
    ) -> Iterator[StatementDict]:
        """One scan of the live view, teeing Arrow batches two ways.

        Each ``(shard, bucket)`` partition streams straight from DuckDB as
        Arrow batches (`_execute_partitioned`). Every batch can go to a
        ``pyarrow`` CSV writer *and* be handed on as row dicts, so a caller
        that wants both ``statements.csv`` and the rows behind it pays for one
        scan rather than writing the csv and reading it back.

        Rows come from ``RecordBatch.to_pylist`` – a bulk conversion in C – and
        carry `STATEMENT_CSV_COLUMNS`, which covers everything an entity
        aggregation needs. They arrive entity-contiguous (the select orders by
        ``entity_id`` and an entity lives in one partition), so
        ``aggregate_unsafe`` can fold them directly.

        The csv handle lives for the generator's lifetime; abandoning the
        generator closes it through the usual ``GeneratorExit`` unwind, so the
        codec trailer is always written.

        Args:
            csv_key: Store key to write the sorted statements csv to.
                ``None`` scans without writing one. Compression comes from
                `compression` (the dataset's config), not from the caller.
            tee: Yield row dicts. ``False`` keeps the scan purely
                columnar – nothing is materialised in Python – which is what
                a csv-only export wants.

        Yields:
            ``StatementDict`` rows, unless ``tee`` is off.
        """
        sql = statement_csv_select()
        # a batch is materialised as Python objects only when rows are asked
        # for, so the cap is on rows-in-flight, not on bytes scanned
        batch_size = SWEEP_BATCH_SIZE if tee else None
        with ExitStack() as stack:
            out = None
            if csv_key is not None:
                out = stack.enter_context(
                    self._store.open(csv_key, "wb", compression=self.compression)
                )
            writer: CSVWriter | None = None
            for reader in self._execute_partitioned(sql, batch_size):
                for batch in reader:
                    if out is not None:
                        if writer is None:
                            writer = CSVWriter(out, batch.schema)
                            # on the stack, so an abandoned generator flushes
                            # the writer's buffer *before* the codec closes
                            stack.callback(writer.close)
                        writer.write(batch)
                    if tee:
                        yield from cast(list[StatementDict], batch.to_pylist())

    def get_entity_ids(
        self, q: Query | None = None, *, source: SqlSource | None = None
    ) -> Iterator[str]:
        """Get entity IDs for given query. Use ``self.source_raw`` to
        target physical storage without tombstones merged"""
        source = source or self.source
        sql = Sql(q or Query(), source=source).canonical_ids
        prune = self._prune_values(q, source)
        for reader in self._execute_partitioned(sql, prune=prune):
            for batch in reader:
                yield from batch["entity_id"].to_pylist()

    def _list_partitions(self) -> list[tuple[str, str, str]]:
        """List all ``(shard, bucket, origin)`` triples currently in the table.

        Read from the snapshot's active files (`_snapshot_pairs`) – metadata,
        no data scan; a ``SELECT DISTINCT`` over ``statement_raw`` opened every
        file of the table to answer this. Every partition holding a file is
        listed, whatever its rows are (pre-merge duplicates, tombstones).
        """
        return sorted(self._snapshot_partitions()[1])

    @staticmethod
    def _prune_values(q: Query | None, source: SqlSource) -> dict[str, set[str]]:
        """Partition values ``q`` can match, per prune column of ``source``.

        The values ftmq folds into the compiled query as ``column IN (...)``
        (``shard`` from an entity id, ``bucket`` from a schema), under the
        same soundness rule: only a plain positive conjunction prunes – below
        ``~`` / ``|`` a filter no longer confines the matching entities to the
        partitions it names. ``_is_flat_and`` is ftmq's own check for that.
        """
        if q is None or not Sql(q, source)._is_flat_and:
            return {}
        return {
            column: set(values)
            for column, prune in source.prune.items()
            if (values := prune(q))
        }

    @staticmethod
    def _pruned_keys(
        pairs: Pairs, prune: dict[str, set[str]] | None
    ) -> list[tuple[str, str]]:
        """The ``(shard, bucket)`` keys of ``pairs`` that ``prune``
        (`_prune_values`) leaves in – all of them without a prune."""
        prune = prune or {}
        return [
            (s, b)
            for s, b in sorted(pairs)
            if s in prune.get("shard", {s}) and b in prune.get("bucket", {b})
        ]

    def _scoped_sources(
        self, prune: dict[str, set[str]] | None = None
    ) -> Iterator[tuple[str, bool]]:
        """Yield one ``(shard, bucket)`` pair's files as a relation, per pair.

        Each relation unions the pair's origin partitions
        ([`partition_source_sql`][ftm_lakehouse.logic.parquet.partition_source_sql]).
        Reads iterate per pair because entity ids (and so statement ids) are
        placed in exactly one ``(shard, bucket)`` by the model layer: one
        pair at a time keeps a full-store ``ORDER BY entity_id`` bounded to a
        partition, and every filter pushes to the files' statistics. The
        snapshot is taken once, so a sweep reads one version of the store.
        Each relation comes with whether every partition in it is clean
        (`Pairs`), which decides the view `_cursor_over` builds on it.

        Pairs outside ``prune`` (`_prune_values`) are skipped rather than
        queried: the compiled query carries the same ``shard IN (...)`` /
        ``bucket IN (...)`` predicate, so they would come back empty – an id
        lookup used to pay a query for every pair of the store.

        A read pruned to shards selects entities by id, so its rows are
        bounded by the ids asked for, not by partition size: every pair it can
        touch goes into one relation and one query. An id alone does not tell
        the bucket, so a lookup touches every bucket of its shard – five
        queries where one does.
        """
        root, pairs = self._snapshot_pairs()

        def union(sources: list[tuple[Partition, Files, bool]]) -> tuple[str, bool]:
            sql = " UNION ALL ".join(
                partition_source_sql(
                    [f"{root}/{file}" for file, _ in files], *partition
                )
                for partition, files, _ in sources
            )
            return f"({sql})", all(clean for _, _, clean in sources)

        keys = self._pruned_keys(pairs, prune)
        if prune and "shard" in prune:
            if keys:
                yield union([source for key in keys for source in pairs[key]])
            return
        for key in keys:
            yield union(pairs[key])

    @contextmanager
    def _cursor_over(
        self, source: str, clean: bool
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """A cursor whose ``statement`` / ``statement_raw`` read ``source``.

        Temporary views, so they shadow the connection's ``delta_scan`` views
        for this cursor only: a query compiled against `TABLE` /
        `TABLE_RAW` runs unchanged, over the files the snapshot named.
        ``statement`` is a plain scan when ``source`` is clean
        ([`live_rows_sql`][ftm_lakehouse.logic.parquet.live_rows_sql]) and the
        dedupe query otherwise
        ([`dedupe_rows_sql`][ftm_lakehouse.logic.parquet.dedupe_rows_sql]) –
        the one place a read consults whether a merge has run, and only to
        pick the cheaper of two equivalent queries.

        Parquet footers are cached: data files are immutable (a rewrite writes
        new ones), so a cached footer never goes stale, and a lookup reads each
        file's footer for the view and again for the query. Set here rather
        than in [`duckdb_config`][ftm_lakehouse.logic.parquet.duckdb_config]:
        as a connect-time option it would make DuckDB autoload the parquet
        extension before it registers, which fails offline.
        """
        with self._lake.cursor() as cur:
            cur.execute("SET parquet_metadata_cache = true")
            cur.execute(
                f"CREATE OR REPLACE TEMP VIEW {TABLE_RAW.name} AS SELECT * FROM {source}"
            )
            live = live_rows_sql if clean else dedupe_rows_sql
            cur.execute(
                f"CREATE OR REPLACE TEMP VIEW {TABLE.name} AS {live(TABLE_RAW.name)}"
            )
            yield cur

    def _execute_partitioned(
        self,
        sql: Select,
        batch_size: int | None = None,
        prune: dict[str, set[str]] | None = None,
    ) -> Iterator[pa.RecordBatchReader]:
        """Yield a streamed Arrow reader per ``(shard, bucket)`` partition.

        Runs ``sql`` over each pair's files (`_scoped_sources`) and hands back
        the result as a lazy `pyarrow.RecordBatchReader` streamed from
        DuckDB's execution pipeline, so memory stays bounded per batch instead
        of materialising the partition.

        Consume each reader fully before advancing to the next: the backing
        cursor is held open only across its ``yield`` and closes when the
        generator resumes for the following partition.

        Args:
            sql: The SQLAlchemy ``Select`` to run.
            batch_size: Rows per Arrow batch. DuckDB's default of 1M is right
                for a purely columnar consumer, but a consumer that turns
                batches into Python objects wants a smaller one – the cap is
                on *materialised rows*, not bytes.
            prune: Partition values the query can match (`_prune_values`) –
                the others are not queried.

        Yields:
            One `pyarrow.RecordBatchReader` per ``(shard, bucket)``
            partition.
        """
        compiled = str(sql.compile(compile_kwargs={"literal_binds": True}))
        for source, clean in self._scoped_sources(prune):
            with self._cursor_over(source, clean) as cur:
                res = cur.execute(compiled)
                if batch_size is None:
                    yield res.to_arrow_reader()
                else:
                    yield res.to_arrow_reader(batch_size)

    def _query_statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Query statement dicts from the live view, bypassing FtM construction.

        Iterates ``(shard, bucket)`` pairs (`_execute_partitioned`), turning
        each Arrow batch into row dicts in one bulk ``to_pylist``.

        Args:
            q: Optional ftmq ``Query`` (default: match-all), compiled via
                `_compile_query`.

        Yields:
            StatementDict instances.
        """
        prune = self._prune_values(q, self.source)
        sql = self._compile_query(q)
        for reader in self._execute_partitioned(sql, SWEEP_BATCH_SIZE, prune):
            for batch in reader:
                yield from cast(list[StatementDict], batch.to_pylist())

    def _query_data(self, q: Query | None = None) -> Iterator[EntityPayload]:
        """
        Query entity dicts via aggregate_unsafe(), bypassing FtM object construction.

        Args:
            q: Optional ftmq ``Query`` (default: match-all), executed via
                `_statement_data`.

        Yields:
            EntityPayload instances
        """
        yield from aggregate_unsafe(self._statement_data(q), self.dataset)
