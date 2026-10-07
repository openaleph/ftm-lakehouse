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
import posixpath
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache, cached_property
from itertools import batched
from threading import RLock
from typing import IO, Any, Callable, Iterable, Iterator, cast
from urllib.parse import unquote

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from anystore.decorators import error_handler
from anystore.interface.lock import Lock
from anystore.io import SyncProgressBar
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
    LakeStore,
    setup_duckdb_storage,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from pyarrow.csv import (  # type: ignore[attr-defined]  # missing from stubs
    CSVWriter,
    WriteOptions,
)
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
    build_merge_sql,
    build_shard_sql,
    dedupe_rows_sql,
    delta_scan_sql,
    duckdb_config,
    live_rows_sql,
    live_view_sql,
    make_prune_by_shard,
    merge_copy_options,
    partition_source_sql,
    raw_view_sql,
    shard_target_file_size,
    worker_duckdb_config,
)
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    TABLE_RAW,
    DeleteCandidate,
    LakehouseStatement,
    deleted_candidates_select,
    statement_csv_select,
)
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.util import process_map, validate_origin

PARTITIONS = ["shard", "bucket", "origin"]

Partition = tuple[str, str, str]
"""A ``(shard, bucket, origin)`` partition key."""

Files = list[tuple[str, int]]
"""A partition's data files as ``(path, size)``: the path table-relative and
percent-decoded – the form DuckDB (prefixed with the table root) and a Delta
``add`` / ``remove`` action take; the log stores it encoded once more, and
``get_add_actions`` hands it back that way."""


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
    """What a worker wrote for one partition.

    ``file`` is the ``(path, size, rows)`` a Delta ``add`` action takes, the
    path table-relative – ``None`` when the merge reaped the partition
    entirely and there is nothing to add. ``took`` is measured in the worker,
    which emits no log lines of its own, so the parent can log it.
    """

    file: tuple[str, int, int] | None
    took: timedelta


Pairs = dict[tuple[str, str], list[tuple[Partition, Files, bool]]]
"""``(partition, files, clean)`` per ``(shard, bucket)`` pair, one entry per
origin partition. ``clean`` is whether every file was written by `merge`
(`MERGED_PREFIX`), i.e. whether a read over it can skip the dedupe."""


def merge_partition(task: MergeTask) -> MergeResult:
    """Merge one partition into a new data file next to its old ones.

    Writes but does not commit: the file is invisible until
    [`ParquetStore.merge`][ParquetStore.merge] commits it, so this runs in a
    worker process as well as in-process, and an uncommitted file is an orphan
    the next ``vacuum`` removes. Reads the partition's files directly
    ([`partition_source_sql`][ftm_lakehouse.logic.parquet.partition_source_sql]),
    so no Delta log is replayed here at all – which is what makes a worker
    cheap: the task carries plain data and the worker opens nothing but its
    own DuckDB connection.

    Deliberately silent. A spawned worker re-imports the library without the
    CLI's logging setup, so ``took`` travels back in the `MergeResult` and the
    parent logs it from `ParquetStore._commit_merged`.

    Args:
        task: The partition, its files, the table root, the grace cutoff and
            the DuckDB config to connect with
            ([`worker_duckdb_config`][ftm_lakehouse.logic.parquet.worker_duckdb_config]).

    Returns:
        `MergeResult` – what to add, and how long it took.
    """
    shard, bucket, origin = task.partition
    config: dict[str, Any] = {**task.duckdb_config}
    with Took() as t, closing(duckdb.connect(config=config)) as con:
        source = partition_source_sql(
            [f"{task.root}/{file}" for file, _ in task.files], shard, bucket, origin
        )
        sql = build_merge_sql(
            shard,
            bucket,
            origin,
            task.grace_cutoff,
            source=source,
            select=f"* EXCLUDE ({', '.join(PARTITIONS)})",
        )
        directory = posixpath.dirname(task.files[0][0])
        file = f"{directory}/{MERGED_PREFIX}{ensure_uuid()}.zstd.parquet"
        target = f"{task.root}/{file}".replace("'", "''")
        # RETURN_STATS: (filename, count, file_size_bytes, ...)
        stats = con.execute(
            f"COPY ({sql}) TO '{target}' ({merge_copy_options(bucket)})"
        ).fetchone()
    if stats and stats[1]:
        return MergeResult((file, int(stats[2]), int(stats[1])), t.took)
    return MergeResult(None, t.took)


def pair_source(
    root: str, sources: list[tuple[Partition, Files, bool]]
) -> tuple[str, bool]:
    """One ``(shard, bucket)`` pair's files as a relation, and whether it is clean.

    The pair's origin partitions unioned
    ([`partition_source_sql`][ftm_lakehouse.logic.parquet.partition_source_sql]),
    with ``clean`` true when every one of them holds only `merge` output – what
    decides the view `register_partition` builds on it.

    Plain data in, plain SQL out, so the parent can resolve a pair against its
    pinned snapshot and hand the result to a worker that replays no Delta log.

    Args:
        root: The table root the file paths are relative to.
        sources: The pair's ``(partition, files, clean)`` triples.

    Returns:
        ``(relation sql, clean)``.
    """
    sql = " UNION ALL ".join(
        partition_source_sql([f"{root}/{file}" for file, _ in files], *partition)
        for partition, files, _ in sources
    )
    return f"({sql})", all(clean for _, _, clean in sources)


def register_partition(
    cur: duckdb.DuckDBPyConnection, source: str, clean: bool
) -> None:
    """Point ``statement`` / ``statement_raw`` at ``source`` on ``cur``.

    Temporary views, so on a cursor of the shared connection they shadow its
    ``delta_scan`` views for that cursor only: a query compiled against
    `TABLE` / `TABLE_RAW` runs unchanged, over the files the snapshot named.
    ``statement`` is a plain scan when ``source`` is clean
    ([`live_rows_sql`][ftm_lakehouse.logic.parquet.live_rows_sql]) and the
    dedupe query otherwise
    ([`dedupe_rows_sql`][ftm_lakehouse.logic.parquet.dedupe_rows_sql]) – the one
    place a read consults whether a merge has run, and only to pick the cheaper
    of two equivalent queries.

    Parquet footers are cached: data files are immutable (a rewrite writes new
    ones), so a cached footer never goes stale, and a lookup reads each file's
    footer for the view and again for the query. Set here rather than in
    [`duckdb_config`][ftm_lakehouse.logic.parquet.duckdb_config]: as a
    connect-time option it would make DuckDB autoload the parquet extension
    before it registers, which fails offline.
    """
    cur.execute("SET parquet_metadata_cache = true")
    cur.execute(
        f"CREATE OR REPLACE TEMP VIEW {TABLE_RAW.name} AS SELECT * FROM {source}"
    )
    live = live_rows_sql if clean else dedupe_rows_sql
    cur.execute(f"CREATE OR REPLACE TEMP VIEW {TABLE.name} AS {live(TABLE_RAW.name)}")


@contextmanager
def partition_cursor(
    source: str, clean: bool, config: dict[str, str]
) -> Iterator[duckdb.DuckDBPyConnection]:
    """A standalone connection whose views read one partition's files.

    What a worker process uses instead of `ParquetStore._cursor_over`: that one
    goes through ftmq's ``LakeStore.cursor``, whose connection loads the
    ``DeltaTable`` and registers ``delta_scan`` views on first use – a log
    replay per process, and the very thing a worker is handed file lists to
    avoid. The session setup is otherwise ftmq's: ``icu`` for a UTC session, so
    ``TIMESTAMPTZ`` does not render in the host timezone, and the storage
    secret for a remote backend.

    Args:
        source: The pair's relation (`pair_source`).
        clean: Whether every partition in it holds only merge output.
        config: DuckDB config to connect with.

    Yields:
        The connection, with ``statement`` / ``statement_raw`` registered.
    """
    duck: dict[str, Any] = {
        "autoinstall_known_extensions": "true",
        "autoload_known_extensions": "true",
        **config,
    }
    with closing(duckdb.connect(":memory:", config=duck)) as con:
        con.execute("LOAD icu; SET GLOBAL TimeZone='UTC'")
        setup_duckdb_storage(con)
        register_partition(con, source, clean)
        yield con


def sweep_partition(
    cur: duckdb.DuckDBPyConnection, out: IO[bytes]
) -> Iterator[StatementDict]:
    """One partition's statements, each Arrow batch also written to ``out`` as
    headerless csv – the export writes the header as a part of its own.

    Rows arrive ordered by ``entity_id``, so ``aggregate_unsafe`` can fold them
    directly. ``out`` stays the caller's to close.
    """
    sql = str(statement_csv_select().compile(compile_kwargs={"literal_binds": True}))
    reader = cur.execute(sql).to_arrow_reader(SWEEP_BATCH_SIZE)
    options = WriteOptions(include_header=False)
    with CSVWriter(out, reader.schema, write_options=options) as writer:
        for batch in reader:
            writer.write(batch)
            yield from cast(list[StatementDict], batch.to_pylist())


@cache
def make_source(table: str, shards: int) -> SqlSource:
    """The `SqlSource` reads compile against, pruning by ``shards``."""
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


class ParquetStore:
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: [`append`][ParquetStore.append] writes each batch as
    new parquet files. Reads reconcile whatever the files hold – duplicates,
    superseded fragments, tombstones – unless a partition is made of
    [`merge`][ParquetStore.merge] output alone, which is canonical and read as
    a plain scan. [`merge`][ParquetStore.merge] and [`vacuum`][ParquetStore.vacuum]
    are therefore maintenance, not a precondition for correct reads.
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
        """The exclusive dataset write fence – ``{dataset_root}/.LOCK``.

        Held by the maintenance that rewrites or drops files in place –
        re-shard, [`delete_origin`][ParquetStore.delete_origin], schema changes,
        [`vacuum`][ParquetStore.vacuum], via `_maintenance_fence` – and by the
        first-ever [`append`][ParquetStore.append] of a dataset (table creation
        must not race). Appends back off while it is held
        (`_await_unlocked`) and take no lock of their own: Delta's optimistic
        concurrency serializes concurrent append commits. [`merge`][ParquetStore.merge]
        does not take it either – see [`merge_lock`][ParquetStore.merge_lock].

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

    def merge_lock(self) -> Lock:
        """The merge lock – ``{dataset_root}/.LOCK-MERGE``.

        Held by [`merge`][ParquetStore.merge] and by an export sweep, and taken
        alongside `_write_lock` by the exclusive maintenance. Appends do not
        wait for it: a merge removes exactly the files it read and an append
        only adds, so Delta commits both whichever lands first, and a read
        reconciles the result – ingest keeps flowing through an hours-long
        merge. A sweep holds it so an ``optimize`` (merge, then a retention-0
        [`vacuum`][ParquetStore.vacuum]) cannot delete the files the sweep's
        snapshot still names.
        """
        return Lock(
            self._store,
            key=path.LOCK_MERGE,
            max_retries=self.settings.lock_max_retries,
        )

    def _await_unlocked(self) -> None:
        """Back off while the exclusive ``.LOCK`` is held, with the fence's
        retry bound. Without a marker of its own, an append that passed this
        check can still be in flight when maintenance takes the lock; for a
        merge that is fine, and the in-place rewrites retry their commit
        ([`delete_origin`][ParquetStore.delete_origin]) or run with writers
        stopped ([`shard`][ParquetStore.shard])."""

        def check() -> None:
            if self._store.exists(path.LOCK):
                raise RuntimeError(
                    f"Write fence busy: maintenance lock `{path.LOCK}` is "
                    "held. If a writer crashed, release the fence via "
                    "`ftm-lakehouse maintenance unlock`."
                )

        self._fence_retry(check)

    def _ensure_table(self) -> None:
        """Create the Delta table (as an empty commit) if it does not exist.

        Runs under the exclusive write lock so two racing first imports
        cannot both commit version ``0``. Establishing existence here –
        once, at the first write – lets [`append`][ParquetStore.append] always
        write with ``mode="append"`` instead of special-casing creation inside
        the hot write path.
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
        """Exclusive fence for maintenance that rewrites or drops files in
        place: the ``.LOCK`` write lock (fencing off other maintenance and new
        appends) plus the merge lock, so a merge or an export sweep is never
        under way at the same time.
        """
        with self._write_lock(), self.merge_lock():
            yield

    def unlock(self) -> bool:
        """Forcibly release the dataset write fence.

        Operator escape hatch for the case where a writer process died
        with a lock held (or an attacker held it on purpose). Releases both
        lock files: the exclusive ``.LOCK`` and the ``.LOCK-MERGE`` of
        [`merge_lock`][ParquetStore.merge_lock].

        **Use sparingly** – breaking a fence that's still held by a live
        writer can corrupt a write in flight. Confirm no process is
        actively writing before running.

        Returns:
            ``True`` if a lock was released, ``False`` if both were clear.
        """
        released = False
        for key in (path.LOCK, path.LOCK_MERGE):
            if self._store.exists(key):
                self._store.delete(key)
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

        Takes no lock: concurrent appends run in parallel – Delta serializes
        their commits via optimistic concurrency – and a concurrent
        [`merge`][ParquetStore.merge] is harmless, since it removes only the
        files it read and a read reconciles the rest. Appends only back off
        while the exclusive ``.LOCK`` of the in-place rewrites is held
        (`_await_unlocked`). Table creation happens once in `_ensure_table`
        (under that lock, so two racing imports can't both commit version
        ``0``); the write loop itself always appends. The files delta-rs
        writes are named
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
            self._await_unlocked()
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
        result. Held under [`merge_lock`][ParquetStore.merge_lock], not the
        exclusive fence: appends keep flowing while this runs.

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
        writes the merged file with DuckDB – and commits the results in batches of
        `MERGE_COMMIT_BATCH` partitions, each batch one Delta transaction of
        ``add`` and ``remove`` actions against that snapshot. A batch is
        atomic: its partitions switch to their merged files together, or not
        at all. A run that committed anything ends with a checkpoint: Delta
        writes one only every hundredth commit, and until then every load
        replays the previous checkpoint – which still lists every file the
        merge just removed, on a store with hundreds of thousands of them –
        plus the merge's commits. The file list is small by now, so the
        checkpoint is cheap, and every load after it is too.

        Partitions merge in ``LAKEHOUSE_WORKERS`` processes (one, the
        default, merges in this process). They are independent – a worker
        writes files and commits nothing – so they parallelise without
        coordination; the DuckDB memory limit and the threads are split
        between the workers
        ([`worker_duckdb_config`][ftm_lakehouse.logic.parquet.worker_duckdb_config]).
        With a partition's dedupe and sort already using every core,
        more workers pay off where the per-partition pipeline leaves cores
        idle – which on a many-partition store is most of a run.

        Memory is bounded by DuckDB itself: the dedupe windows and the final
        sort spill to ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY`` past each worker's
        share of ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``. A partition too large to
        merge in acceptable time wants more shards (`shard`), not a smaller
        merge.

        Args:
            force: Rewrite every partition, clean ones included – with
                ``LAKEHOUSE_GRACE_PERIOD_DAYS=0`` that purges cold tombstones.
        """
        if not self.exists:
            return
        grace_cutoff = utc_now() - timedelta(days=self.settings.grace_period_days)
        workers = max(self.settings.workers, 1)
        config = worker_duckdb_config(workers)
        merged = skipped = 0
        with self.merge_lock():
            root, partitions = self._snapshot_partitions()
            tasks = [
                MergeTask(partition, files, root, grace_cutoff, config)
                for partition, (files, clean) in partitions.items()
                if force or not clean
            ]
            skipped = len(partitions) - len(tasks)
            # one bar, advanced per partition, its throughput the bytes read
            with (
                SyncProgressBar("Merging partitions", len(tasks)) as bar,
                process_map(workers) as run,
            ):

                def results() -> Iterator[tuple[MergeTask, MergeResult]]:
                    for task, result in zip(tasks, run(merge_partition, tasks)):
                        bar.advance(size=sum(size for _, size in task.files))
                        yield task, result

                for batch in batched(results(), MERGE_COMMIT_BATCH):
                    self._commit_merged(batch)
                    merged += len(batch)
            if merged:
                with self._snapshot_lock, Took() as t:
                    snapshot = self._current_snapshot()
                    if snapshot is not None:
                        snapshot.create_checkpoint()
                self.log.info("Wrote checkpoint.", took=t.took)
        self.log.info(
            "Merge complete.",
            merged=merged,
            skipped=skipped,
            workers=workers,
            grace_period_days=self.settings.grace_period_days,
        )

    def _commit_merged(self, batch: Iterable[tuple[MergeTask, MergeResult]]) -> None:
        """Commit a batch of merged partitions as one Delta transaction.

        Each partition's merged file is added and every file it was merged
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
            if result.file is not None:
                file, size, rows = result.file
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
        keep their combined buffers bounded; the follow-up ``merge`` rewrites
        each partition into one file anyway.

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
        under the exclusive maintenance fence (`_maintenance_fence`), like the
        other in-place rewrites; an append to the same origin that was already
        in flight when the fence closed conflicts with the delete's commit,
        which is retried under the fence's bound.

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
            if self._current_snapshot() is None:
                return 0
            metrics: dict[str, Any] = {}

            def drop() -> None:
                nonlocal metrics
                snapshot = self._current_snapshot()
                assert snapshot is not None
                # safe to interpolate: `validate_origin` rejects quotes
                metrics = snapshot.delete(f"origin = '{origin}'")

            self._fence_retry(drop)
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

    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files no longer referenced by the Delta log.

        Files [`merge`][ParquetStore.merge] replaced become orphans on disk;
        vacuum prunes them once they're past
        ``retention_hours``. Held under the exclusive maintenance fence
        (`_maintenance_fence`).

        Args:
            retention_hours: Keep files newer than this many hours. ``0``
                drops every file the Delta log no longer references.
        """
        if not self.exists:
            return
        deleted: list[str] = []
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is not None:
                deleted = snapshot.vacuum(
                    retention_hours=retention_hours,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
        self.log.info("Vacuumed.", files=len(deleted), took=t.took)

    def sweep_sources(self) -> list[tuple[tuple[str, str], str, bool]]:
        """Every ``(shard, bucket)`` pair of this process's snapshot, resolved.

        What a parallel sweep fans out over: one relation per pair
        (`pair_source`), taken from **one** snapshot so every worker reads the
        same version of the store. An entity id is placed in exactly one pair,
        so a pair is a unit no entity spans – which is what lets each worker
        fold entities on its own.

        Returns:
            ``((shard, bucket), relation sql, clean)`` per pair, in key order.
        """
        root, pairs = self._snapshot_pairs()
        return [(key, *pair_source(root, pairs[key])) for key in sorted(pairs)]

    def deleted_candidates(self, since: datetime) -> Iterator[DeleteCandidate]:
        """Every entity carrying a tombstone at or after ``since``.

        One `deleted_candidates_select` pass over the raw view of every
        partition – ``deleted_at`` is no partition column, so there is nothing
        to prune by and the pass is the whole table either way. That is why it
        is one pass: a diff series needs its tombstoned ids before the sweep
        opens, and asking per series paid for this scan per series.

        Args:
            since: Earliest tombstone any caller cares about – the earliest
                window of the series being served.

        Yields:
            `DeleteCandidate`, one per entity.
        """
        sql = deleted_candidates_select(since)
        for reader in self._execute_partitioned(sql, batch_size=SWEEP_BATCH_SIZE):
            for batch in reader:
                for row in batch.to_pylist():
                    yield DeleteCandidate(
                        id=row["entity_id"],
                        deleted_at=row["deleted_at"],
                        origins=frozenset(row["origins"] or ()),
                        schemata=frozenset(row["schemata"] or ()),
                        content_hash=bool(row["content_hash"]),
                    )

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
        keys = self._pruned_keys(pairs, prune)
        if prune and "shard" in prune:
            if keys:
                yield pair_source(root, [s for key in keys for s in pairs[key]])
            return
        for key in keys:
            yield pair_source(root, pairs[key])

    @contextmanager
    def _cursor_over(
        self, source: str, clean: bool
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """A cursor of the shared connection, its views reading ``source``.

        `register_partition` does the work; this is the in-process half of the
        pair, where a worker process has `partition_cursor` instead.
        """
        with self._lake.cursor() as cur:
            register_partition(cur, source, clean)
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
