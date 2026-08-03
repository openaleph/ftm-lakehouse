"""ParquetStore – Delta Lake table with entity-hash shard partitioning.

Statements live in one Delta Lake table (per dataset) partitioned by
``(shard, bucket, origin)``. ``shard`` is the hex-padded entity_id hash bucket;
the uniform shard count is set per dataset via ``DatasetModel.shards``.

Writes are **append-only**: each flush sorts a per-partition batch by
``(entity_id, id, last_seen DESC)`` in memory and appends it as a new parquet
file. Two views are registered on the underlying ``LakeStore`` connection –
:func:`~ftm_lakehouse.logic.parquet.live_view_sql` produces the ``statement``
view that every read targets (a plain ``WHERE deleted_at IS NULL`` scan), and
:func:`~ftm_lakehouse.logic.parquet.raw_view_sql` produces ``statement_raw``
for code paths that need tombstones and pre-merge duplicates visible
(:meth:`merge`, :meth:`get_changed_entity_ids`).

**Correctness assumes an optimized store.** The live view has no read-time
dedupe – it just hides tombstones – so reads are correct only once
:meth:`merge` has made the store canonical (one row per statement id,
fragment supersession applied, ``first_seen`` / ``last_seen`` folded). All of
that dedupe logic lives solely in
:func:`~ftm_lakehouse.logic.parquet.build_merge_sql`. Between a write and the
next merge, reads can surface duplicate ids and rows whose delete has not been
applied yet.

Statement-level reads iterate ``(shard, bucket)`` partitions and add
``WHERE shard = ? AND bucket = ?`` per query, keeping a full-store ``ORDER BY
entity_id`` bounded to one partition; any filter pushes through the plain
scan to DuckDB's file statistics. ``stats()`` and ``view()`` go through the
un-iterated global view.

``merge`` collapses physical duplicates and reaps tombstones past grace –
load-bearing for read correctness, not just cleanup; ``compact`` bin-packs
small files; ``vacuum`` removes obsolete Delta file versions.

Layout:
    entities/statements/shard={s}/bucket={b}/origin={o}/part-*.parquet
"""

import random
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator, cast
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
from anystore.interface.lock import Lock
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import Took, join_uri, mask_uri
from deltalake import DeltaTable, write_deltalake
from followthemoney import StatementEntity
from followthemoney.statement import StatementDict
from ftmq.model.stats import DatasetStats
from ftmq.query import Query, Sql, SqlSource
from ftmq.store.lake import (
    TARGET_SIZE,
    LakeQueryView,
    LakeStatement,
    LakeStore,
    get_schema_bucket,
    setup_duckdb_storage,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from ftmq.util import make_dataset
from pyarrow.csv import CSVWriter  # type: ignore[attr-defined]  # missing from stubs
from sqlalchemy import Select, column, or_, select

from ftm_lakehouse.core.api import LakehouseApiMixin, no_api
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.compress import CompressKind, compress_stream
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.parquet import (
    build_changed_sql,
    build_merge_sql,
    duckdb_config,
    live_view_sql,
    raw_view_sql,
)
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    TABLE_RAW,
    statement_csv_select,
)
from ftm_lakehouse.storage.tags import TagStore

PARTITIONS = ["shard", "bucket", "origin"]

STATEMENT_SOURCE = SqlSource(
    TABLE,
    id_column="entity_id",
    prune={"schema": get_schema_bucket},
    prune_column="bucket",
)
"""ftmq compile target for the live ``statement`` view.

The lakehouse sharded table keyed on ``entity_id`` – physical storage carries no
``canonical_id`` – with a schema filter folded into a ``bucket IN (...)``
partition-prune predicate on every compiled query (ftmq's
:class:`~ftmq.query.SqlSource` ``prune``). Replaces the removed
``ensure_schema_buckets`` helper and the ``Query.table`` mutation."""


class ParquetStore(LakehouseApiMixin):
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: :meth:`append` sorts a per-partition batch in
    memory and writes one parquet file. Reads target the live ``statement``
    view (``deleted_at IS NULL``) registered on the :class:`LakeStore`
    connection and assume a store made canonical by :meth:`merge` –
    :meth:`merge`, :meth:`compact`, :meth:`vacuum` are load-bearing for read
    correctness, not just cleanup.
    """

    def __init__(
        self,
        uri: Uri,
        dataset: str,
        shards: int | None = None,
        compression: CompressKind | None = None,
    ) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        super().__init__(self.uri)
        self.settings = Settings()
        self.dataset = dataset
        self.shards = shards if shards is not None else DEFAULT_SHARDS
        # Resolved from the dataset config by the owning repository – exports
        # never take a runtime codec (see `repository.base.resolve_compression`).
        self.compression = compression
        self._store = get_store(uri)
        self._tags = TagStore(uri)
        self._lake = LakeStore(
            uri=str(self.uri),
            dataset=self.dataset,
            partition_by=PARTITIONS,
            view_sqls={
                TABLE.name: live_view_sql,
                TABLE_RAW.name: raw_view_sql,
            },
            duckdb_config=duckdb_config(),
        )
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            uri=mask_uri(self.uri),
        )
        setup_duckdb_storage()

    @property
    def deltatable(self) -> DeltaTable:
        return self._lake.deltatable

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        if self._lake.exists:
            return self._lake.deltatable.version()

    @property
    def exists(self) -> bool:
        """Check existence of deltatable"""
        return self._lake.exists

    @no_api
    def view(self) -> LakeQueryView:
        """Get a view for querying statements."""
        return self._lake.default_view()

    @no_api
    def get(self, entity_id: str) -> StatementEntity | None:
        """Lookup an Entity by its ID"""
        stmts = list(self.get_statements(entity_id))
        if stmts:
            return StatementEntity.from_statements(make_dataset(self.dataset), stmts)

    def compile_query(self, q: Query | None = None) -> Select:
        """Compile ``q`` to a statements ``Select`` against the live view.

        Compiles through :data:`STATEMENT_SOURCE`, so a schema filter folds into
        a ``bucket IN (...)`` predicate (ftmq's :class:`~ftmq.query.SqlSource`
        ``prune``) and a schema-scoped read prunes to the matching bucket
        partitions instead of scanning all of them. The single entry point every
        lakehouse read funnels its :class:`~ftmq.query.Query` through.
        """
        q = q or Query()
        return q.compile(STATEMENT_SOURCE)

    @staticmethod
    def _needs_global(q: Query | None) -> bool:
        """Whether ``q`` must execute as ONE query over the whole view.

        The compiled ``LIMIT`` / ``OFFSET`` live in ftmq's un-scoped
        ``canonical_ids`` subquery and ``ORDER BY`` only orders within a
        partition, so under the per-``(shard, bucket)`` iteration a sliced
        or sorted query would over-return (one limit *per partition*) and
        mis-order. Those queries bypass the iteration and run globally via
        ``LakeStore._execute`` – bounded by the limit (DuckDB top-N) resp.
        an inherent global sort.
        """
        return q is not None and (q.sort is not None or q.slice is not None)

    def _global_statement_data(self, sel: Select) -> Iterator[StatementDict]:
        """Execute a compiled select ONCE over the whole live view.

        Entity rows stay contiguous for aggregation: ftmq's statement
        selects order by ``entity_id`` (unsorted) or ``(sortable_value,
        id)`` (sorted).
        """
        for row in self._lake._execute(sel):
            yield cast(StatementDict, vars(row))

    @no_api
    def query(
        self, q: Query | None = None, origin: str | None = None
    ) -> StatementEntities:
        """Query entities from the store.

        Args:
            q: Optional ``Query`` of entity-level filters (schema, properties,
                ids, ...) plus ordering / slicing – a sorted or sliced query
                executes globally (:meth:`_needs_global`) so ``LIMIT`` and
                ``ORDER BY`` hold across partitions.
            origin: Optional storage-level row filter – restrict to statements
                of this origin, so an assembled entity carries only that
                origin's statements.

        Yields:
            StatementEntity objects matching the query.
        """
        sel = self.compile_query(q)
        if origin is not None:
            sel = sel.where(column("origin") == origin)
        if self._needs_global(q):
            rows = self._global_statement_data(sel)
            for data in aggregate_unsafe(rows, self.dataset):
                yield data.to_entity()
        else:
            for data in self._query_data(sel):
                yield data.to_entity()

    @no_api
    def query_statements(
        self, q: Query | None = None, origin: str | None = None
    ) -> Statements:
        """Query ordered Statements from the store.

        Args:
            q: Optional ``Query`` – compiled through :meth:`compile_query`;
                sorted / sliced queries execute globally
                (:meth:`_needs_global`).
            origin: Optional storage-level row filter.

        Yields:
            :class:`~ftmq.store.lake.LakeStatement` objects matching the
            query.
        """
        sel = self.compile_query(q)
        if origin is not None:
            sel = sel.where(column("origin") == origin)
        if self._needs_global(q):
            for stmt_dict in self._global_statement_data(sel):
                yield LakeStatement.from_dict(stmt_dict)
        else:
            for stmt_dict in self._query_statement_data(sel):
                yield LakeStatement.from_dict(stmt_dict)

    @no_api
    def get_statements(self, entity_id: str) -> Statements:
        """Query all live statements for a single entity.

        Scopes :meth:`_query_statement_data` iteration to the entity's
        own shard so single-entity lookups don't fan out to every
        ``(shard, bucket)`` pair. Yields
        :class:`ftmq.store.lake.LakeStatement` so the ``fragment`` group
        key stays visible – tombstone writers rely on it so a delete
        lands in the same supersession group as the live row.
        """
        if not self.exists:
            return
        shard = path.entity_shard(entity_id, self.shards)
        q = select(TABLE).where(TABLE.c.shard == shard, TABLE.c.entity_id == entity_id)
        for stmt_dict in self._query_statement_data(q, shard=shard):
            yield LakeStatement.from_dict(stmt_dict)

    @no_api
    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store.

        Runs ftmq's aggregation SQL over the live ``statement`` view. Assumes
        an optimized store: the live view is a plain ``deleted_at IS NULL``
        scan, so the aggregates are correct only once :meth:`merge` has made
        the store canonical (one row per id, supersession applied). Run
        ``optimize`` before heavy stats workloads.
        """
        return self._lake.default_view().stats()

    @no_api
    def count(self, q: Query | None = None) -> int:
        """Count distinct entities matching ``q``.

        A single ``count(DISTINCT entity_id)`` aggregate (not the
        per-partition read iteration), so it's cheap enough to short-circuit an
        export that would otherwise iterate every partition for zero results.
        Compiled through :data:`STATEMENT_SOURCE`, so a schema filter folds into
        the same ``bucket IN (...)`` prune as :meth:`compile_query` –
        non-matching partitions are pruned, not just file-skipped. Like the
        other aggregates it assumes an optimized store.
        """
        if not self.exists:
            return 0
        q = q or Query()
        for row in self._lake._execute(Sql(q, STATEMENT_SOURCE).count):
            for value in row:
                return int(value)
        return 0

    def _write_lock(self) -> Lock:
        """Exclusive side of the dataset write fence.

        Held by maintenance (:meth:`merge`, :meth:`compact`, :meth:`vacuum`
        via :meth:`_maintenance_fence`) and by the first-ever :meth:`append`
        of a dataset (table creation must not race). The lock lives at
        ``{dataset_root}/.LOCK`` per ``path.LOCK``.

        Regular appends do **not** take this lock – they register a shared
        marker instead (:meth:`_append_fence`); Delta's optimistic
        concurrency serializes concurrent append commits safely on its own.

        Acquisition is bounded by ``settings.lock_max_retries`` (total wait
        roughly ``N²/2`` seconds); entering the returned lock raises
        ``RuntimeError`` when the fence stays busy, so contended writers fail
        instead of pinning a thread forever. A lock left behind by a crashed
        writer must be released manually via :meth:`unlock`
        (``ftm-lakehouse operations unlock``).
        """
        return Lock(
            self._store, key=path.LOCK, max_retries=self.settings.lock_max_retries
        )

    def _await(self, ready: Callable[[], bool], what: str) -> None:
        """Block until ``ready()`` is true, with the fence's retry bound.

        Linear backoff matching the ``Lock`` acquisition semantics
        (anystore's ``error_handler`` with ``backoff_random``): attempt
        ``N`` sleeps ``N`` seconds plus up to one second of jitter – so
        concurrent waiters don't wake in lockstep – and
        ``settings.lock_max_retries`` retries wait roughly ``N²/2`` seconds
        in total before raising ``RuntimeError``.
        """
        retries = 0
        while not ready():
            retries += 1
            if retries > self.settings.lock_max_retries:
                raise RuntimeError(
                    f"Write fence busy: {what}. If a writer crashed, release "
                    "the fence via `ftm-lakehouse operations unlock`."
                )
            time.sleep(retries + random.random())

    def _append_markers(self) -> list[str]:
        """Keys of all currently registered append markers."""
        return list(self._store.iterate_keys(prefix=path.LOCK_APPENDS))

    @contextmanager
    def _append_fence(self) -> Iterator[None]:
        """Shared (append) side of the dataset write fence.

        Registers a marker key under ``.LOCK-APPENDS/`` and only *then*
        checks the maintenance ``.LOCK`` – the store-then-load order makes
        the handshake sound on a linearizable store: when the ``.LOCK``
        check sees no lock, the marker write is already visible to any
        later drain poll by a maintenance holder, so
        :meth:`_maintenance_fence` can never pass its drain while an
        unnoticed append is in flight. When ``.LOCK`` is held, the marker
        is removed *before* backing off (a parked appender must not
        deadlock the drain), then register-and-check retries under the
        fence's usual bound.

        Concurrent appends never block each other – Delta append commits
        are blind appends that delta-rs serializes via optimistic commit
        retries. A marker left behind by a crashed appender blocks
        maintenance until released via :meth:`unlock`
        (``ftm-lakehouse operations unlock``).
        """
        marker = f"{path.LOCK_APPENDS}/{uuid4().hex}"
        retries = 0
        while True:
            self._store.touch(marker)
            if not self._store.exists(path.LOCK):
                break
            self._store.delete(marker, ignore_errors=True)
            retries += 1
            if retries > self.settings.lock_max_retries:
                raise RuntimeError(
                    f"Write fence busy: maintenance lock `{path.LOCK}` is "
                    "held. If a writer crashed, release the fence via "
                    "`ftm-lakehouse operations unlock`."
                )
            time.sleep(retries + random.random())
        try:
            yield
        finally:
            self._store.delete(marker, ignore_errors=True)

    def _ensure_table(self) -> None:
        """Create the Delta table (as an empty commit) if it does not exist.

        Runs under the exclusive write lock so two racing first imports
        cannot both commit version ``0``. Establishing existence here –
        once, at the first write – lets :meth:`append` always take the
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

    @no_api
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

    @no_api
    def append(self, batch: pa.Table) -> None:
        """Append a sorted batch of statements.

        The batch should be scoped to a single ``shard`` for write efficiency
        (one parquet file per ``(shard, bucket, origin)`` partition). The
        method sorts by ``(bucket, origin, entity_id, fragment, prop, id,
        last_seen DESC)`` – clustering a fragment's rows physically
        contiguous, then by ``prop`` because the supersession group key
        includes it – then splits by ``bucket`` so each ``write_deltalake``
        call uses the bucket-appropriate ``writer_properties`` (small vs.
        large profile). Duplicates land as separate rows and are reaped by
        :meth:`merge`.

        Held under the *shared* side of the write fence
        (:meth:`_append_fence`): concurrent appends run in parallel – Delta
        serializes their commits via optimistic concurrency – while
        :meth:`merge` / :meth:`compact` / :meth:`vacuum` wait for the append
        markers to drain before rewriting partitions. Table creation happens
        once in :meth:`_ensure_table` (under the exclusive lock, so two
        racing imports can't both commit version ``0``); the write loop
        itself always appends. Each touched ``(shard, bucket, origin)``
        partition is stamped with a ``last_updated`` freshness tag *before*
        the Delta writes so a later :meth:`merge` can skip partitions that
        didn't change – see :meth:`_mark_updated` for the crash-safety
        ordering.

        Args:
            batch: PyArrow table with the columns of
                :data:`ftm_lakehouse.model.statement.SHARDED_SCHEMA`. Rows
                should already be scoped to a single shard.
        """
        if len(batch) == 0:
            return

        buckets = pc.unique(batch["bucket"]).to_pylist()
        shards = pc.unique(batch["shard"]).to_pylist()
        self.log.info(
            f"Flushing {len(batch)} statements to parquet ...",
            buckets=buckets,
            shards=shards,
        )
        batch = batch.sort_by(
            [
                ("bucket", "ascending"),
                ("origin", "ascending"),
                ("entity_id", "ascending"),
                ("fragment", "ascending"),
                ("prop", "ascending"),
                ("id", "ascending"),
                ("last_seen", "descending"),
            ]
        )
        with self._tags.touch(tag.STATEMENTS_UPDATED):
            self._mark_updated(batch)
            self._ensure_table()
            with self._append_fence():
                for bucket in buckets:
                    sub = batch.filter(pc.equal(batch["bucket"], bucket))
                    write_deltalake(
                        str(self.uri),
                        sub,
                        partition_by=PARTITIONS,
                        mode="append",
                        writer_properties=writer_for_bucket(bucket),
                        storage_options=storage_options(),
                    )

    def _mark_updated(self, batch: pa.Table) -> None:
        """Stamp a ``last_updated`` tag on every partition present in ``batch``.

        Partition-level counterpart to the dataset-wide
        :data:`~ftm_lakehouse.core.conventions.tag.STATEMENTS_UPDATED` tag:
        one tag per distinct ``(shard, bucket, origin)`` triple in the
        batch. :meth:`merge` compares each partition's ``last_updated``
        against its ``last_optimized`` to decide whether the partition
        needs rewriting.

        Called at the head of :meth:`append`, inside the write fence but
        *before* the Delta commits – the conservative crash ordering: a
        writer dying mid-append leaves at worst a dirty tag with no data
        (one harmless extra merge), never committed data in a
        clean-looking partition that merge would skip forever (reads
        depend on merge for correctness, so a permanently skipped
        partition would surface duplicates indefinitely).
        """
        partitions = batch.select(PARTITIONS).group_by(PARTITIONS).aggregate([])
        for shard, bucket, origin in zip(
            partitions["shard"].to_pylist(),
            partitions["bucket"].to_pylist(),
            partitions["origin"].to_pylist(),
        ):
            self._tags.set(tag.statements_partition_updated(shard, bucket, origin))

    @no_api
    def merge(self, grace_period_days: int | None = None, force: bool = False) -> None:
        """Collapse duplicates and reap expired tombstones, partition by partition.

        For each ``(shard, bucket, origin)`` partition, runs the merge
        query against ``statement_raw`` (non-fragment rows: keep latest
        row per ``id`` by ``last_seen DESC``; fragment rows: keep the
        latest emission per ``(entity_id, prop, fragment)`` group; fold
        ``first_seen`` to the min; drop tombstones older than the grace
        cutoff) and atomically overwrites that partition via
        ``partition_filters``. Held under the exclusive maintenance fence
        (``path.LOCK`` + append-marker drain, :meth:`_maintenance_fence`).

        Only partitions whose ``last_updated`` freshness tag is newer than
        their ``last_optimized`` tag are rewritten – a partition untouched
        since its last merge is skipped, so an optimize after a small
        append rewrites only what changed instead of the whole store. Each
        successful rewrite stamps ``last_optimized`` (and back-fills a
        missing ``last_updated``, so partitions predating freshness
        tracking are merged once and then skipped instead of rewritten on
        every run).

        Because a clean partition is never revisited by a *default* merge,
        a tombstone sitting in an otherwise-idle partition is not
        physically reaped once it passes the grace window until the next
        write touches that partition – this only defers disk reclamation;
        read correctness is unaffected (the live view hides tombstones
        regardless). Passing an explicit ``grace_period_days`` bypasses
        the skip and re-evaluates every partition, so a purge
        (``grace_period_days=0``) physically reaps cold tombstones too.

        Load-bearing for reads: the live ``statement`` view does no
        dedupe, so a partition's rows are only canonical – one row per id,
        fragment supersession applied, ``first_seen`` / ``last_seen``
        folded – after this runs. Reads assume every touched partition has
        been merged since its last write.

        Args:
            grace_period_days: Override ``settings.grace_period_days``. Pass
                ``0`` to drop tombstones immediately. An explicit value
                forces every partition to be rewritten (grace is evaluated
                against all tombstones, not just dirty partitions).
            force: Rewrite every partition regardless of freshness tags.
        """
        if not self.exists:
            return
        days = (
            grace_period_days
            if grace_period_days is not None
            else self.settings.grace_period_days
        )
        # An explicit grace bound means "re-evaluate every partition now" -
        # the skip would otherwise leave tombstones in clean partitions
        # unreaped (a grace=0 purge must physically drop them everywhere).
        force = force or grace_period_days is not None
        grace_cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        merged = skipped = 0
        with self._maintenance_fence():
            for shard, bucket, origin in self._list_partitions():
                updated = tag.statements_partition_updated(shard, bucket, origin)
                optimized = tag.statements_partition_optimized(shard, bucket, origin)
                if not (force or not self._tags.is_latest(optimized, [updated])):
                    skipped += 1
                    continue
                # Back-fill the dep tag for partitions written before
                # freshness tracking - stamped before ``touch(optimized)``
                # captures its (later) timestamp, so the strict `>` in
                # ``is_latest`` skips this partition next run.
                # FIXME
                if not self._tags.exists(updated):
                    self._tags.set(updated)
                with Took() as t, self._tags.touch(optimized):
                    sql = build_merge_sql(shard, bucket, origin, grace_cutoff)
                    with self._lake.cursor() as cur:
                        # ``to_arrow_reader`` yields a pyarrow RecordBatchReader
                        # that DuckDB streams lazily from its execution
                        # pipeline; ``write_deltalake`` consumes the reader
                        # batch by batch, so the merge never materialises the
                        # full partition in Python memory.
                        reader = cur.execute(sql).to_arrow_reader()
                        write_deltalake(
                            str(self.uri),
                            reader,
                            mode="overwrite",
                            partition_by=PARTITIONS,
                            predicate=(
                                f"shard = '{shard}' AND bucket = '{bucket}' "
                                f"AND origin = '{origin}'"
                            ),
                            writer_properties=writer_for_bucket(bucket),
                            target_file_size=TARGET_SIZE,
                            storage_options=storage_options(),
                        )
                    merged += 1
                    self.log.info(
                        f"Merged partition `{shard}/{bucket}/{origin}`.",
                        took=t.took,
                        shard=shard,
                        bucket=bucket,
                        origin=origin,
                        grace_period_days=days,
                    )
            if merged:
                # A rewrite changes the store's logical content (duplicates
                # collapse, deletes apply), so consumers keyed on
                # STATEMENTS_UPDATED - exports, statistics, the fresh-CSV
                # shortcut - must go stale and re-run after an optimize.
                self._tags.set(tag.STATEMENTS_UPDATED)
        self.log.info(
            "Merge complete.", merged=merged, skipped=skipped, grace_period_days=days
        )

    @no_api
    def compact(self) -> None:
        """Bin-pack small parquet files within each partition.

        Cheap maintenance – Delta's ``OPTIMIZE compact`` only rewrites small
        files into larger ones; it does not collapse duplicate rows or drop
        tombstones (use :meth:`merge` for that). Held under the exclusive
        maintenance fence (:meth:`_maintenance_fence`).
        """
        if not self.exists:
            return
        with self._maintenance_fence():
            with Took() as t:
                for shard, bucket, origin in self._list_partitions():
                    self.deltatable.optimize.compact(
                        partition_filters=[
                            ("shard", "=", shard),
                            ("bucket", "=", bucket),
                            ("origin", "=", origin),
                        ],
                        writer_properties=writer_for_bucket(bucket),
                        target_size=TARGET_SIZE,
                    )
            self.log.info("Compaction done.", took=t.took)

    @no_api
    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files no longer referenced by the Delta log.

        Tombstoned files (replaced by :meth:`merge` / :meth:`compact`) become
        orphans on disk; vacuum prunes them once they're past
        ``retention_hours``. Held under the exclusive maintenance fence
        (:meth:`_maintenance_fence`).

        Args:
            retention_hours: Keep files newer than this many hours. ``0``
                drops every file the Delta log no longer references.
        """
        if not self.exists:
            return
        with self._maintenance_fence(), Took() as t:
            self.deltatable.vacuum(
                retention_hours=retention_hours,
                dry_run=False,
                enforce_retention_duration=False,
            )
            self.log.info("Vacuumed.", took=t.took)

    @no_api
    def export_csv(self, key: str, q: Select | None = None) -> None:
        """Export statements to a sorted CSV file.

        Streams each ``(shard, bucket)`` partition straight from DuckDB as
        Arrow batches (:meth:`_execute_partitioned`) into a ``pyarrow`` CSV
        writer, so the export stays vectorised end to end – no per-row
        Python materialisation. Memory stays bounded per batch and the
        ``ORDER BY entity_id`` sort stays bounded to one partition.

        Compression comes from :attr:`compression` (the dataset's config), not
        from the caller.

        Args:
            q: Optional SQLAlchemy select (default:
                :func:`~ftm_lakehouse.model.statement.statement_csv_select` –
                the FtM columns plus ``fragment``, ordered by ``entity_id``).
            split: Split artifact into chunks (by partitions)
        """
        if not self.exists:
            return
        if q is None:
            q = statement_csv_select()
        with (
            self._store.open(key, "wb") as fh,
            compress_stream(fh, self.compression) as out,
        ):
            writer: CSVWriter | None = None
            for reader in self._execute_partitioned(q):
                for batch in reader:
                    if writer is None:
                        writer = CSVWriter(out, batch.schema)
                    writer.write(batch)
            if writer is not None:
                writer.close()

    @no_api
    def get_changed_entity_ids(
        self,
        since: datetime,
        schemata: list[str] | None = None,
        prop: str | None = None,
    ) -> Iterator[str]:
        """Get entity IDs touched since a timestamp.

        Catches both *new* / *modified* statements (``first_seen >= since``)
        and *deleted* ones (``deleted_at >= since``) – the latter so the diff
        consumer can emit DEL ops for entities whose tombstone landed after
        the last diff state. Targets ``statement_raw`` because the deduped
        view filters tombstones; we need them visible here.
        """
        if not self.exists:
            return

        since_truncated = since.replace(microsecond=0)
        sql = (
            select(TABLE_RAW)
            .distinct(TABLE_RAW.c.entity_id)
            .where(
                or_(
                    TABLE_RAW.c.first_seen >= since_truncated,
                    TABLE_RAW.c.deleted_at >= since_truncated,
                )
            )
        )
        if schemata:
            sql = sql.where(TABLE_RAW.c.schema.in_(schemata))
        if prop:
            sql = sql.where(TABLE_RAW.c.prop == prop)
        seen: set[str] = set()
        for shard, _bucket in self._iter_shard_buckets():
            scoped = sql.where(TABLE_RAW.c.shard == shard)
            for row in self._lake._execute(scoped):
                if row.entity_id not in seen:
                    seen.add(row.entity_id)
                    yield row.entity_id

    @no_api
    def destroy(self) -> None:
        """
        Destroy the deltalake by removing the transaction log in "_delta_log"
        directory. This is soft deleting, as the parquet files remain (but will
        be cleaned up on optimize --vacuum)
        """
        with Took() as t:
            self.log.warn("🔥 Destroying deltalake store ...")
            for key in self._lake._backend.iterate_keys("_delta_log"):
                self._lake._backend.delete(key)
        self.log.info("Deleted statement store.", took=t.took)

    def _list_partitions(self) -> list[tuple[str, str, str]]:
        """List all ``(shard, bucket, origin)`` triples currently in the table.

        Queries ``statement_raw`` so the enumeration scans the underlying
        Delta partitions directly, seeing pre-merge duplicates and
        tombstones (the live view hides tombstones).
        """
        if not self.exists:
            return []
        with self._lake.cursor() as cur:
            rows = cur.execute(
                f"SELECT DISTINCT shard, bucket, origin FROM {TABLE_RAW.name} "
                "ORDER BY shard, bucket, origin"
            ).fetchall()
        return [(s, b, o) for s, b, o in rows]

    def _iter_shard_buckets(
        self, shard: str | None = None
    ) -> Iterator[tuple[str, str]]:
        """Yield unique ``(shard, bucket)`` pairs from existing partitions.

        Reads (:meth:`_query_statement_data`) iterate per ``(shard,
        bucket)`` because entity IDs (and thus statement IDs) are uniquely
        placed in one ``(shard, bucket)`` by the model layer. Adding
        ``WHERE shard = ? AND bucket = ?`` per iteration keeps a full-store
        ``ORDER BY entity_id`` bounded to one partition and lets the
        predicate push through the live view's plain scan to the parquet
        file statistics.

        Args:
            shard: Optional shard filter. When given, only ``(shard,
                bucket)`` pairs for that shard are yielded – lets
                single-entity lookups skip the other shards.
        """
        seen: set[tuple[str, str]] = set()
        for s, b, _origin in self._list_partitions():
            if shard is not None and s != shard:
                continue
            key = (s, b)
            if key not in seen:
                seen.add(key)
                yield s, b

    def _execute_partitioned(
        self, q: Select | None = None, *, shard: str | None = None
    ) -> Iterator[pa.RecordBatchReader]:
        """Yield a streamed Arrow reader per ``(shard, bucket)`` partition.

        Scopes ``q`` with ``shard = ? AND bucket = ?`` per partition – so a
        full-store ``ORDER BY`` stays bounded to one partition and the
        predicate pushes through the live ``statement`` view's plain scan to
        the parquet file statistics – and hands back each partition's result
        as a lazy :class:`pyarrow.RecordBatchReader` streamed from DuckDB's
        execution pipeline, so memory stays bounded per batch instead of
        materialising the partition.

        Consume each reader fully before advancing to the next: the backing
        cursor is held open only across its ``yield`` and closes when the
        generator resumes for the following partition.

        Args:
            q: Optional SQLAlchemy select (default: :meth:`compile_query`).
            shard: Optional shard filter to scope iteration to one shard.

        Yields:
            One :class:`pyarrow.RecordBatchReader` per ``(shard, bucket)``
            partition.
        """
        if q is None:
            q = self.compile_query()
        for s, b in self._iter_shard_buckets(shard=shard):
            scoped = q.where(column("shard") == s, column("bucket") == b)
            sql = str(scoped.compile(compile_kwargs={"literal_binds": True}))
            with self._lake.cursor() as cur:
                yield cur.execute(sql).to_arrow_reader()

    def _query_statement_data(
        self, q: Select | None = None, *, shard: str | None = None
    ) -> Iterator[StatementDict]:
        """Query statement dicts from the live view, bypassing FtM construction.

        Iterates over ``(shard, bucket)`` partitions, scoping each query with
        ``WHERE shard = ? AND bucket = ?`` so a full-store ``ORDER BY
        entity_id`` stays bounded to one partition (an entity lives in one
        ``(shard, bucket)``). The live ``statement`` view is a plain
        ``deleted_at IS NULL`` scan, so any ftmq filter (``schema`` / ``prop``
        / ``entity_id``) pushes straight through to ``delta_scan``'s per-file
        statistics. Correctness assumes an optimized store – on an un-merged
        store this can surface duplicate ids and rows whose delete has not been
        applied yet.

        Args:
            q: Optional SQLAlchemy select (default: :meth:`compile_query`).
            shard: Optional shard filter passed through to
                :meth:`_iter_shard_buckets` to scope iteration to one shard –
                used by single-entity lookups.

        Yields:
            StatementDict instances.
        """
        if q is None:
            q = self.compile_query()
        for s, b in self._iter_shard_buckets(shard=shard):
            scoped = q.where(column("shard") == s, column("bucket") == b)
            for row in self._lake._execute(scoped):
                yield StatementDict(**vars(row))

    def _query_data(self, q: Select | None = None) -> Iterator[EntityPayload]:
        """
        Query entity dicts via aggregate_unsafe(), bypassing FtM object construction.

        Args:
            q: Optional SQLAlchemy select (default: compile_query())

        Yields:
            EntityPayload instances
        """
        if not self.exists:
            return
        yield from aggregate_unsafe(self._query_statement_data(q), self.dataset)

    @no_api
    def query_changed(self, since: datetime) -> Iterator[EntityPayload]:
        """Aggregate the canonical state of entities changed since ``since`` –
        any entity with a statement whose ``first_seen`` or ``deleted_at`` is
        newer.

        Runs :func:`~ftm_lakehouse.logic.parquet.build_changed_sql` per
        ``(shard, bucket)`` partition: the raw view scoped by a *semi-join
        subquery* on the changed entity ids (no huge ``IN (...)`` literal,
        which segfaults DuckDB on large id sets), pushed through the same
        two-branch dedupe as physical merge and filtered to live rows. So
        the result matches a post-merge read **without requiring a merge
        first** – a deleted-but-unmerged entity yields zero rows (its
        tombstones shadow the live rows), which is what lets the diff
        exporter emit ``DEL`` ops on an un-merged store, and an updated
        entity aggregates only its superseded-away latest values.

        An entity lives in exactly one ``(shard, bucket)``, so its
        statements are all present per partition and ``aggregate_unsafe``
        sees each entity contiguous (the ``ORDER BY entity_id`` stays
        partition-bounded).
        """
        if not self.exists:
            return
        since_truncated = since.replace(microsecond=0)
        for s, b in self._iter_shard_buckets():
            sql = build_changed_sql(s, b, since_truncated)
            yield from aggregate_unsafe(self._execute_sql(sql), self.dataset)

    def _execute_sql(self, sql: str) -> Iterator[StatementDict]:
        """Stream raw-SQL results as ``StatementDict`` rows.

        Counterpart to ``LakeStore._execute`` for SQL strings the
        SQLAlchemy layer cannot express (the dedupe CTEs of
        :func:`~ftm_lakehouse.logic.parquet.build_changed_sql`). The
        cursor stays pinned in this generator's frame while its result
        streams via ``fetchmany``.
        """
        with self._lake.cursor() as cur:
            res = cur.execute(sql)
            cols = [d[0] for d in res.description]
            while rows := res.fetchmany(100_000):
                for row in rows:
                    yield cast(StatementDict, dict(zip(cols, row)))
