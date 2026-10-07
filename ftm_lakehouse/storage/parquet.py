"""ParquetStore – one Delta Lake table per dataset, partitioned by
``(shard, bucket, origin)``.

Writes are append-only; reads reconcile duplicates, superseded fragments and
tombstones per ``(shard, bucket)`` pair, unless a partition holds only `merge`
output, which reads as a plain scan. `merge` and `vacuum` are maintenance, not
a precondition for correct reads; `shard` is the one operation that moves rows
between partitions.

Layout:
    statements/shard={s}/bucket={b}/origin={o}/{part,merged}-*.parquet
"""

import json
import posixpath
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from itertools import chain
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
    merge_sorted,
    partition_source_sql,
    raw_view_sql,
    shard_target_file_size,
    split_duckdb_config,
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
from ftm_lakehouse.util import prefetch, process_map, validate_origin

PARTITIONS = ["shard", "bucket", "origin"]

Partition = tuple[str, str, str]
"""A ``(shard, bucket, origin)`` partition key."""

Files = list[tuple[str, int]]
"""A partition's data files as ``(path, size)`` – the path table-relative and
percent-decoded, as DuckDB and Delta ``add`` / ``remove`` actions take it."""


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
    """What a worker wrote for one partition: the ``(path, size, rows)`` of the
    merged file, ``None`` when the merge reaped it entirely, and how long it
    took – workers do not log."""

    partition: Partition
    file: tuple[str, int, int] | None
    took: timedelta


Partitions = dict[Partition, tuple[Files, bool]]
"""``(files, clean)`` per partition – ``clean`` when every file was written by
`merge` (`MERGED_PREFIX`), so a read over it can skip the dedupe."""

Pairs = dict[tuple[str, str], list[tuple[Partition, Files, bool]]]
"""`Partitions` grouped per ``(shard, bucket)`` pair."""


def _compile(sql: Select) -> str:
    """``sql`` as DuckDB SQL, parameters inlined."""
    return str(sql.compile(compile_kwargs={"literal_binds": True}))


def _merged(file: str) -> bool:
    """Whether ``file`` was written by `merge` (`MERGED_PREFIX`)."""
    return posixpath.basename(file).startswith(MERGED_PREFIX)


def merge_partition(task: MergeTask) -> MergeResult:
    """Merge one partition into a new data file next to its old ones.

    Writes but does not commit – [`ParquetStore.merge`][ParquetStore.merge]
    does, and an uncommitted file is an orphan the next ``vacuum`` removes.
    Reads the files directly, so a worker replays no Delta log.
    """
    shard, bucket, origin = task.partition
    config: dict[str, Any] = {**task.duckdb_config}
    with Took() as t, closing(duckdb.connect(config=config)) as con:
        con.execute("SET enable_progress_bar = false")  # see `partition_cursor`
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
        return MergeResult(task.partition, (file, int(stats[2]), int(stats[1])), t.took)
    return MergeResult(task.partition, None, t.took)


def _relations(root: str, sources: list[tuple[Partition, Files, bool]]) -> list[str]:
    """One relation per partition."""
    return [
        partition_source_sql([f"{root}/{file}" for file, _ in files], *partition)
        for partition, files, _ in sources
    ]


def _union(relations: Iterable[str]) -> str:
    """``relations`` as one relation."""
    return f"({' UNION ALL '.join(relations)})"


def pair_source(
    root: str, sources: list[tuple[Partition, Files, bool]]
) -> tuple[str, bool]:
    """One ``(shard, bucket)`` pair's origin partitions as one relation, and
    whether all of them are clean."""
    return _union(_relations(root, sources)), all(clean for _, _, clean in sources)


@dataclass(frozen=True)
class SweepSource:
    """One ``(shard, bucket)`` pair as the export sweeps it – plain data,
    resolved against one snapshot, so it pickles to a worker."""

    key: tuple[str, str]
    relations: list[str]
    """One relation per origin partition."""
    clean: bool
    """Every partition holds only `merge` output."""
    presorted: bool
    """Every partition is one `merge` file, written in ``entity_id`` order – so
    the sweep merges the partitions' streams instead of sorting the pair."""
    size: int
    """Bytes of the pair's files."""

    @property
    def relation(self) -> str:
        """The pair as one relation."""
        return _union(self.relations)


def register_partition(
    cur: duckdb.DuckDBPyConnection, source: str, clean: bool
) -> None:
    """Point the ``statement`` / ``statement_raw`` temp views at ``source``.

    ``statement`` is a plain scan over a clean source, the dedupe query
    otherwise. The parquet footer cache is set here, not in the connect-time
    config, where it would autoload the parquet extension too early (offline).
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
    """A standalone connection with `register_partition`'s views over
    ``source`` – unlike ``LakeStore.cursor`` it loads no ``DeltaTable``."""
    duck: dict[str, Any] = {
        "autoinstall_known_extensions": "true",
        "autoload_known_extensions": "true",
        **config,
    }
    with closing(duckdb.connect(":memory:", config=duck)) as con:
        # a spawned worker's DuckDB takes itself for interactive (its `__main__`
        # has no `__file__` at import) and draws its own bar onto the terminal;
        # a session setting, refused as a connect-time option
        con.execute("SET enable_progress_bar = false")
        con.execute("LOAD icu; SET GLOBAL TimeZone='UTC'")
        setup_duckdb_storage(con)
        register_partition(con, source, clean)
        yield con


@contextmanager
def sweep_batches(
    source: SweepSource, config: dict[str, str]
) -> Iterator[Iterator[pa.RecordBatch]]:
    """A pair's live statements (`statement_csv_select`) as Arrow batches in
    ``entity_id`` order, each read a batch ahead on a thread (`prefetch`).

    A presorted pair streams every partition in file order and merges the
    streams (`merge_sorted`), its memory bounded by the batches in flight. Any
    other pair is sorted as one relation – a DuckDB sort over a union does not
    spill, so that needs the whole pair in memory.
    """
    select = statement_csv_select()
    with ExitStack() as stack:
        if source.presorted:
            sql = _compile(select.order_by(None))  # file order is entity order
            configs = split_duckdb_config(config, len(source.relations))
            streams = []
            for relation, share in zip(source.relations, configs):
                cur = stack.enter_context(partition_cursor(relation, True, share))
                reader = cur.execute(sql).to_arrow_reader(SWEEP_BATCH_SIZE)
                streams.append(stack.enter_context(prefetch(reader)))
            yield merge_sorted(streams)
        else:
            cur = stack.enter_context(
                partition_cursor(source.relation, source.clean, config)
            )
            reader = cur.execute(_compile(select)).to_arrow_reader(SWEEP_BATCH_SIZE)
            yield stack.enter_context(prefetch(reader))


def sweep_partition(
    batches: Iterable[pa.RecordBatch], out: IO[bytes]
) -> Iterator[StatementDict]:
    """A pair's statements (`sweep_batches`), each batch also written to ``out``
    as headerless csv – the export writes the header as a part of its own.

    Rows arrive ordered by ``entity_id``, so ``aggregate_unsafe`` can fold them
    directly. ``out`` stays the caller's to close.
    """
    options = WriteOptions(include_header=False)
    with ExitStack() as stack:
        writer = None
        for batch in batches:
            if writer is None:
                writer = stack.enter_context(
                    CSVWriter(out, batch.schema, write_options=options)
                )
            writer.write(batch)
            yield from cast(list[StatementDict], batch.to_pylist())


@cache
def make_source(shards: int) -> SqlSource:
    """The `SqlSource` reads compile against, pruning by ``shards``."""
    prune = {**PRUNE, "shard": make_prune_by_shard(shards)}
    return SqlSource(TABLE, id_column="entity_id", prune=prune)


class _LakeStore(LakeStore):
    """ftmq's store, ``exists`` answered from the owning store's snapshot rather
    than by loading a ``DeltaTable`` per ``stats()`` aggregate."""

    def __init__(self, *args: Any, exists: Callable[[], bool], **kwargs: Any) -> None:
        self._exists = exists
        super().__init__(*args, **kwargs)

    @property
    def exists(self) -> bool:
        return self._exists()


class ParquetStore:
    """The statement store of one dataset: one Delta table, partitioned by
    ``(shard, bucket, origin)``."""

    def __init__(
        self,
        uri: Uri,
        dataset: str,
        shards: int | None = None,
    ) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        self.settings = Settings()
        self.dataset = dataset
        self.shards = shards if shards is not None else DEFAULT_SHARDS
        self._store = get_store(uri)
        self._tags = TagStore(uri)
        self._snapshot_lock = RLock()
        self._snapshot: DeltaTable | None = None
        self._files: tuple[int, str, Partitions] | None = None
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
        """A freshly loaded handle on the table, for callers outside the store."""
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
        """Whether the Delta table exists."""
        with self._snapshot_lock:
            return self._current_snapshot() is not None

    def _current_snapshot(self) -> DeltaTable | None:
        """This process's Delta snapshot, advanced with ``update_incremental`` –
        loading one replays the whole checkpoint. Callers hold `_snapshot_lock`.
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

    def _partitions(self) -> tuple[str, Partitions]:
        """The table root and the snapshot's files per partition, from its add
        actions – regrouped only when the version moved; empty without a table."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return "", {}
            version = snapshot.version()
            if self._files is None or self._files[0] != version:
                actions = pa.table(snapshot.get_add_actions(flatten=True))
                files: dict[Partition, Files] = {}
                for file, size, *partition in zip(
                    actions["path"].to_pylist(),
                    actions["size_bytes"].to_pylist(),
                    actions["partition.shard"].to_pylist(),
                    actions["partition.bucket"].to_pylist(),
                    actions["partition.origin"].to_pylist(),
                ):
                    key = cast(Partition, tuple(partition))
                    files.setdefault(key, []).append((unquote(file), size))
                partitions = {
                    key: (fs, all(_merged(f) for f, _ in fs))
                    for key, fs in sorted(files.items())
                }
                root = snapshot.table_uri.rstrip("/")
                self._files = (version, root, partitions)
            return self._files[1], self._files[2]

    def _pairs(self) -> tuple[str, Pairs]:
        """`_partitions`, grouped per ``(shard, bucket)`` pair."""
        root, partitions = self._partitions()
        pairs: Pairs = {}
        for partition, (files, clean) in partitions.items():
            pairs.setdefault(partition[:2], []).append((partition, files, clean))
        return root, pairs

    @property
    def source(self) -> SqlSource:
        return make_source(self.shards)

    def _statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Statement dicts for ``q``, entity-contiguous – per ``(shard, bucket)``
        pair, or as one ``delta_scan`` query when sorted or sliced, so ``LIMIT``
        and ``ORDER BY`` hold across partitions."""
        sql = (q or Query()).compile(self.source)
        prune = self._prune_values(q, self.source)
        if q is not None and (q.sort is not None or q.slice is not None):
            root, pairs = self._pairs()
            if not root:
                return
            keys = self._pruned_keys(pairs, prune)
            clean = all(c for key in keys for _, _, c in pairs[key])
            sources: Iterable[tuple[str, bool]] = [(delta_scan_sql(root), clean)]
        else:
            sources = self._scoped_sources(prune)
        yield from cast(Iterator[StatementDict], self._rows(sql, sources))

    def query(self, q: Query | None = None) -> StatementEntities:
        """Query entities from the store.

        Args:
            q: Filters, plus ordering / slicing.

        Yields:
            `StatementEntity` objects.
        """
        for data in aggregate_unsafe(self._statement_data(q), self.dataset):
            yield data.to_entity()

    def query_statements(self, q: Query | None = None) -> Statements:
        """Query statements from the store.

        Args:
            q: Filters, plus ordering / slicing.

        Yields:
            `LakehouseStatement` objects – with their ``fragment`` and ``role``,
            so one can be handed to
            [`delete_statement`][ftm_lakehouse.repository.EntityRepository.delete_statement].
        """
        for stmt_dict in self._statement_data(q):
            yield LakehouseStatement.from_dict(stmt_dict)

    def stats(self) -> DatasetStats:
        """ftmq's statistics over the reconciling connection-level ``statement``
        view."""
        return self._lake.default_view().stats()

    def _write_lock(self) -> Lock:
        """The exclusive ``.LOCK`` – held by the in-place rewrites and table
        creation. Appends back off while it is held (`_await_unlocked`); a
        crashed holder needs [`unlock`][ParquetStore.unlock]."""
        return Lock(
            self._store, key=path.LOCK, max_retries=self.settings.lock_max_retries
        )

    def _fence_retry(self, attempt: Callable[[], None]) -> None:
        """Retry ``attempt`` until it stops raising – ``lock_max_retries``
        attempts, ``N²/2`` seconds in total, then the error propagates."""
        error_handler(
            max_retries=self.settings.lock_max_retries,
            backoff_factor=1,
            do_raise=True,
        )(attempt)()

    def merge_lock(self) -> Lock:
        """The ``.LOCK-MERGE`` – held by [`merge`][ParquetStore.merge] and the export
        sweep, and by the exclusive maintenance alongside ``.LOCK``. Appends
        don't wait for it; it keeps an ``optimize`` from vacuuming files a
        running sweep still reads."""
        return Lock(
            self._store,
            key=path.LOCK_MERGE,
            max_retries=self.settings.lock_max_retries,
        )

    def _await_unlocked(self) -> None:
        """Back off while ``.LOCK`` is held. An append that passed this may still be
        in flight when maintenance takes the lock: `delete_origin` retries its
        commit, `shard` runs with writers stopped."""

        def check() -> None:
            if self._store.exists(path.LOCK):
                raise RuntimeError(
                    f"Write fence busy: maintenance lock `{path.LOCK}` is "
                    "held. If a writer crashed, release the fence via "
                    "`ftm-lakehouse maintenance unlock`."
                )

        self._fence_retry(check)

    def _ensure_table(self) -> None:
        """Create the table as an empty commit, under ``.LOCK`` so two first
        imports cannot both commit version ``0``."""
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
        """``.LOCK`` plus ``.LOCK-MERGE``, for maintenance that rewrites or drops
        files in place."""
        with self._write_lock(), self.merge_lock():
            yield

    def unlock(self) -> bool:
        """Forcibly release ``.LOCK`` and ``.LOCK-MERGE`` after a crashed writer.

        Breaking a lock a live writer holds can corrupt its write – confirm no
        process is writing first.

        Returns:
            Whether a lock was released.
        """
        released = False
        for key in (path.LOCK, path.LOCK_MERGE):
            if self._store.exists(key):
                self._store.delete(key)
                released = True
        return released

    def evolve_schema(self) -> list[str]:
        """Add the `SHARDED_SCHEMA` columns the table was created without.

        A metadata-only commit; older files read the new columns as NULL.
        Additive only – a removed column needs a full rewrite.

        Returns:
            The columns added.
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
        """Apply `TABLE_CONFIGURATION`, then checkpoint and clean up the log under
        it.

        Returns:
            The properties that changed.
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
        """Derive ``shard`` from ``entity_id`` – the only place a row's partition is
        decided, so it always follows this store's configured count. Hashes the
        distinct ids only."""
        ids = pc.dictionary_encode(batch.column("entity_id").combine_chunks())
        shards = pa.array(
            [entity_shard(e, self.shards) for e in ids.dictionary.to_pylist()],
            pa.string(),
        )
        return batch.append_column(
            SHARDED_SCHEMA.field("shard"), pc.take(shards, ids.indices)
        ).select(SHARDED_SCHEMA.names)

    def append(self, batch: pa.Table) -> None:
        """Append `JOURNAL_SCHEMA` rows, deriving their ``shard``.

        One write per bucket, for its writer profile; each becomes a file per
        partition it touches. Unsorted and lock-free – concurrent appends commit
        through Delta's optimistic concurrency; only ``.LOCK`` makes them wait.
        The files come out ``part-*``, so their partitions are dirty until the
        next [`merge`][ParquetStore.merge].
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
        """Whether any partition holds a file [`merge`][ParquetStore.merge] did not
        write."""
        return not all(clean for _, clean in self._partitions()[1].values())

    def merge(self, force: bool = False) -> None:
        """Rewrite every dirty partition into one canonical file.

        Duplicates collapse, fragments supersede, ``first_seen`` folds and
        tombstones past ``LAKEHOUSE_GRACE_PERIOD_DAYS`` go, together with the
        rows they shadow ([`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql]).
        Reads return the same rows before and after, so the commits carry
        ``dataChange = false``.

        Partitions merge in ``LAKEHOUSE_WORKERS`` processes from one snapshot and
        are committed in batches of `MERGE_COMMIT_BATCH`, followed by a
        checkpoint. A run that fails still commits the partitions that finished,
        so the next one picks up the rest. Held under
        [`merge_lock`][ParquetStore.merge_lock] only, so appends keep flowing.

        Args:
            force: Rewrite clean partitions too – with a grace period of ``0``
                that purges tombstones in otherwise idle partitions.
        """
        if not self.exists:
            return
        grace_cutoff = utc_now() - timedelta(days=self.settings.grace_period_days)
        workers = max(self.settings.workers, 1)
        merged = skipped = 0
        with self.merge_lock():
            root, partitions = self._partitions()
            tasks = [
                MergeTask(
                    partition, files, root, grace_cutoff, worker_duckdb_config(workers)
                )
                for partition, (files, clean) in partitions.items()
                if force or not clean
            ]
            skipped = len(partitions) - len(tasks)
            # one bar, advanced per partition, its throughput the bytes read
            with (
                SyncProgressBar("Merging partitions", len(tasks)) as bar,
                process_map(workers, ordered=False) as run,
            ):
                by_partition = {task.partition: task for task in tasks}
                done: list[tuple[MergeTask, MergeResult]] = []
                try:
                    for result in run(merge_partition, tasks):
                        task = by_partition[result.partition]
                        bar.advance(size=sum(size for _, size in task.files))
                        done.append((task, result))
                        if len(done) == MERGE_COMMIT_BATCH:
                            batch, done = done, []
                            self._commit_merged(batch)
                            merged += len(batch)
                finally:
                    # also when a partition failed or a worker died: what finished
                    # is committed, so the next run resumes instead of starting over
                    if done:
                        self._commit_merged(done)
                        merged += len(done)
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
        """Commit a batch of merged partitions as one transaction: each merged file
        added, the files it replaced removed, all ``dataChange = false``."""
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

    def shard(self, shards: int) -> None:
        """Rewrite the store onto ``shards`` entity-hash shards.

        One streamed, atomic overwrite per ``(bucket, origin)`` group – neither
        deduped nor sorted, so every partition comes out dirty: run ``optimize``
        afterwards. Idempotent. Blocks parquet appends but not the journal, so
        run it with writers stopped.

        Args:
            shards: Target shard count; ``<= 1`` means a single shard.
        """
        if self.exists:
            self._rewrite_shards(shards)
        self.shards = shards
        self.log.info("Re-shard complete.", shards=shards)

    def _rewrite_shards(self, shards: int) -> None:
        """Rewrite every ``(bucket, origin)`` group from one snapshot's files, under
        the maintenance fence."""
        with self._maintenance_fence():
            root, partitions = self._partitions()
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
                    # lazily, one query at a time: a connection streams one
                    # result, and executing the next one closes it
                    readers = (con.execute(sql).to_arrow_reader() for sql in sqls)
                    first = next(readers)
                    batches = chain.from_iterable(chain([first], readers))
                    write_deltalake(
                        snapshot,
                        pa.RecordBatchReader.from_batches(first.schema, batches),
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
        """Physically drop every row of one origin – whole partitions, at once,
        with no grace period.

        Args:
            origin: The origin to drop.

        Returns:
            Number of rows removed.

        Raises:
            ValueError: If ``origin`` is not a safe origin name.
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
        """Delete the files the Delta log no longer references.

        Args:
            retention_hours: Keep such files newer than this.
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

    def sweep_sources(self) -> list[SweepSource]:
        """Every ``(shard, bucket)`` pair of one snapshot as a ``SweepSource`` –
        the units an export sweeps, no entity spanning two."""
        root, pairs = self._pairs()
        return [
            SweepSource(
                key=key,
                relations=_relations(root, pairs[key]),
                clean=all(clean for _, _, clean in pairs[key]),
                presorted=all(clean and len(fs) == 1 for _, fs, clean in pairs[key]),
                size=sum(size for _, files, _ in pairs[key] for _, size in files),
            )
            for key in sorted(pairs)
        ]

    def deleted_candidates(self, since: datetime) -> Iterator[DeleteCandidate]:
        """Every entity with a tombstone at or after ``since`` – one raw scan over
        the whole store, shared by every diff series of an export."""
        sql = deleted_candidates_select(since)
        for row in self._rows(sql, self._scoped_sources()):
            yield DeleteCandidate(
                id=row["entity_id"],
                deleted_at=row["deleted_at"],
                origins=frozenset(row["origins"] or ()),
                schemata=frozenset(row["schemata"] or ()),
                content_hash=bool(row["content_hash"]),
            )

    @staticmethod
    def _prune_values(q: Query | None, source: SqlSource) -> dict[str, set[str]]:
        """The partition values ``q`` confines its matches to, per prune column –
        only for a flat positive conjunction, as ftmq prunes."""
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
        """The keys of ``pairs`` that ``prune`` leaves in."""
        prune = prune or {}
        return [
            (s, b)
            for s, b in sorted(pairs)
            if s in prune.get("shard", {s}) and b in prune.get("bucket", {b})
        ]

    def _scoped_sources(
        self, prune: dict[str, set[str]] | None = None
    ) -> Iterator[tuple[str, bool]]:
        """One ``(relation, clean)`` per ``(shard, bucket)`` pair ``prune`` leaves in.

        A read pruned to shards is bounded by its ids, so all its pairs go into
        one relation – an id names its shard but not its bucket.
        """
        root, pairs = self._pairs()
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
        """A cursor of the shared connection, its views reading ``source``
        (`register_partition`)."""
        with self._lake.cursor() as cur:
            register_partition(cur, source, clean)
            yield cur

    def _rows(
        self, sql: Select, sources: Iterable[tuple[str, bool]]
    ) -> Iterator[dict[str, Any]]:
        """``sql``'s rows over each ``(relation, clean)`` source in turn
        (`_scoped_sources`), in Arrow batches of `SWEEP_BATCH_SIZE`."""
        compiled = _compile(sql)
        for source, clean in sources:
            with self._cursor_over(source, clean) as cur:
                for batch in cur.execute(compiled).to_arrow_reader(SWEEP_BATCH_SIZE):
                    yield from batch.to_pylist()
