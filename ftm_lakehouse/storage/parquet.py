"""ParquetStore – Delta Lake table with entity-hash shard partitioning.

Statements live in one Delta Lake table (per dataset) partitioned by
``(shard, bucket, origin)``. ``shard`` is the hex-padded entity_id hash bucket;
the uniform shard count is set per dataset via ``DatasetModel.shards``. It is
derived in [`ParquetStore.append`][ParquetStore.append] and nowhere else – producers hand over
``JOURNAL_SCHEMA`` rows with no shard key at all, so a partition can never be
picked against a shard count other than the one this store is configured for.

Writes are **append-only**: each flush sorts a per-partition batch by
``(entity_id, id, last_seen DESC)`` in memory and appends it as a new parquet
file. Two views are registered on the underlying ``LakeStore`` connection –
[`live_view_sql`][ftm_lakehouse.logic.parquet.live_view_sql] produces the ``statement``
view that every read targets (a plain ``WHERE deleted_at IS NULL`` scan), and
[`raw_view_sql`][ftm_lakehouse.logic.parquet.raw_view_sql] produces ``statement_raw``
for code paths that need tombstones and pre-merge duplicates visible
(`merge`, `get_entity_ids` over `source_raw`).

**Correctness assumes an optimized store.** The live view has no read-time
dedupe – it just hides tombstones – so reads are correct only once
`merge` has made the store canonical (one row per statement id,
fragment supersession applied, ``first_seen`` / ``last_seen`` folded). All of
that dedupe logic lives solely in
[`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql]. Between a write and the
next merge, reads can surface duplicate ids and rows whose delete has not been
applied yet.

Statement-level reads iterate ``(shard, bucket)`` partitions and add
``WHERE shard = ? AND bucket = ?`` per query, keeping a full-store ``ORDER BY
entity_id`` bounded to one partition; any filter pushes through the plain
scan to DuckDB's file statistics. ``stats()`` and ``view()`` go through the
un-iterated global view.

``merge`` collapses physical duplicates and reaps tombstones past grace –
load-bearing for read correctness, not just cleanup; ``compact`` bin-packs
small files; ``vacuum`` removes obsolete Delta file versions. ``shard``
re-keys the whole store onto a different shard count, the one operation
that moves rows between partitions.

Layout:
    statements/shard={s}/bucket={b}/origin={o}/part-*.parquet
"""

from contextlib import contextmanager
from datetime import timedelta
from functools import cache, cached_property
from typing import Callable, Iterator, cast
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from anystore.decorators import error_handler
from anystore.interface.lock import Lock
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import Took, join_uri, mask_uri
from deltalake import DeltaTable, Schema, write_deltalake
from followthemoney.statement import StatementDict
from ftmq.model.stats import DatasetStats
from ftmq.query import Query, Sql, SqlSource
from ftmq.store.base import View
from ftmq.store.lake import (
    PRUNE,
    TARGET_SIZE,
    LakeStore,
    setup_duckdb_storage,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from pyarrow.csv import CSVWriter  # type: ignore[attr-defined]  # missing from stubs
from rigour.time import utc_now
from sqlalchemy import Select, column

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.compress import CompressKind, compress_stream
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.parquet import (
    build_bounds_sample_sql,
    build_merge_sql,
    build_shard_sql,
    duckdb_config,
    live_view_sql,
    make_prune_by_shard,
    merge_slice_count,
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


@cache
def make_source(table: str, shards: int) -> SqlSource:
    """Create `SqlSource` (live or raw) with configured shards"""
    config = {
        "id_column": "entity_id",
        "prune": {**PRUNE, "shard": make_prune_by_shard(shards)},
    }
    return SqlSource(table, **config)


class ParquetStore:
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: [`append`][ParquetStore.append] sorts a per-partition batch in
    memory and writes one parquet file. Reads target the live ``statement``
    view (``deleted_at IS NULL``) registered on the `LakeStore`
    connection and assume a store made canonical by
    [`merge`][ParquetStore.merge] – [`merge`][ParquetStore.merge],
    [`compact`][ParquetStore.compact], [`vacuum`][ParquetStore.vacuum] are
    load-bearing for read correctness, not just cleanup.
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

    def view(self) -> View:
        """Get a view for querying statements."""
        return self._lake.default_view()

    @cached_property
    def source(self) -> SqlSource:
        return make_source(TABLE, self.shards)

    @cached_property
    def source_raw(self) -> SqlSource:
        return make_source(TABLE_RAW, self.shards)

    def _compile_query(
        self, q: Query | None = None, *, source: SqlSource | None = None
    ) -> Select:
        """Compile ``q`` to a statements ``Select`` against the live view.

        Compiles through `self.source`, so a schema filter folds into
        a ``bucket IN (...)`` predicate (ftmq's `SqlSource`
        ``prune``) and a schema-scoped read prunes to the matching bucket
        partitions instead of scanning all of them. The single entry point every
        lakehouse read funnels its `Query` through.
        """
        if q is None:
            q = Query()
        return q.compile(source or self.source)

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

    def _global_statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Execute a compiled select ONCE over the whole live view.

        Entity rows stay contiguous for aggregation: ftmq's statement
        selects order by ``entity_id`` (unsorted) or ``(sortable_value,
        id)`` (sorted).
        """
        for row in self._lake._execute(self._compile_query(q)):
            yield cast(StatementDict, vars(row))

    def _statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Statement dicts for ``q``, choosing the execution strategy.

        One global query when sort / slice demand it (`_needs_global`),
        else the per-partition iteration (`_query_statement_data`).
        Rows stay entity-contiguous either way, so aggregation can run over
        the stream directly. Empty for a store that has never been written.
        """
        if not self.exists:
            return
        if self._needs_global(q):
            yield from self._global_statement_data(q)
        else:
            yield from self._query_statement_data(q)

    def query(self, q: Query | None = None) -> StatementEntities:
        """Query entities from the store.

        Args:
            q: Optional ``Query`` of entity-level filters (schema, properties,
                ids, ...) plus ordering / slicing – a sorted or sliced query
                executes globally (`_needs_global`) so ``LIMIT`` and
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
                sorted / sliced queries execute globally
                (`_needs_global`).

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

        Runs ftmq's aggregation SQL over the live ``statement`` view. Assumes
        an optimized store: the live view is a plain ``deleted_at IS NULL``
        scan, so the aggregates are correct only once [`merge`][ParquetStore.merge] has made
        the store canonical (one row per id, supersession applied). Run
        ``optimize`` before heavy stats workloads.
        """
        return self._lake.default_view().stats()

    def count(self, q: Query | None = None) -> int:
        """Count distinct entities matching ``q``.

        A single ``count(DISTINCT entity_id)`` aggregate (not the
        per-partition read iteration), so it's cheap enough to short-circuit an
        export that would otherwise iterate every partition for zero results.
        Compiled through `self.source`, so a schema filter folds into
        the same ``bucket IN (...)`` prune as `_compile_query` –
        non-matching partitions are pruned, not just file-skipped. Like the
        other aggregates it assumes an optimized store.
        """
        if not self.exists:
            return 0
        if q is None:
            q = Query()
        for row in self._lake._execute(Sql(q, self.source).count):
            for value in row:
                return int(value)
        return 0

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
        return list(self._store.iterate_keys(prefix=path.LOCK_APPENDS))

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
        if not self.exists:
            return []
        deltatable = self.deltatable
        known = {f.name for f in deltatable.schema().fields}
        missing = [
            f for f in Schema.from_arrow(SHARDED_SCHEMA).fields if f.name not in known
        ]
        if not missing:
            return []
        names = [f.name for f in missing]
        with self._maintenance_fence():
            deltatable.alter.add_columns(missing)
        self.log.info("Evolved parquet schema.", columns=names)
        return names

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
            [path.entity_shard(e, self.shards) for e in ids.dictionary.to_pylist()],
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
        itself always appends. Each touched ``(shard, bucket, origin)``
        partition is stamped with a ``last_updated`` freshness tag inside
        the fence and *before* the Delta writes, so a later [`merge`][ParquetStore.merge]
        can skip partitions that didn't change – see `_mark_updated`
        for why both halves of that ordering are load-bearing.

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
                self._mark_updated(batch)
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
        [`STATEMENTS_UPDATED`][ftm_lakehouse.core.conventions.tag.STATEMENTS_UPDATED] tag:
        one tag per distinct ``(shard, bucket, origin)`` triple in the
        batch. [`merge`][ParquetStore.merge] compares each partition's ``last_updated``
        against its ``last_optimized`` to decide whether the partition
        needs rewriting.

        Called *inside* the append fence and *before* the Delta commits.
        Both halves matter, and the failure they prevent is the same one:
        a partition that looks clean while holding un-merged rows, which a
        default [`merge`][ParquetStore.merge] then skips forever (reads depend on merge
        for correctness, so it would surface duplicates indefinitely).

        - Before the commits, so a writer dying mid-append leaves at worst
          a dirty tag with no data – one harmless extra merge.
        - Inside the fence, so a [`merge`][ParquetStore.merge] cannot stamp
          ``last_optimized`` between this tag and the commits it belongs
          to. Outside the fence that interleaving is reachable: the
          appender stamps ``last_updated``, gets locked out of the fence
          by the in-flight merge, and commits its rows only after that
          merge has stamped a *newer* ``last_optimized`` over them.
        """
        partitions = batch.select(PARTITIONS).group_by(PARTITIONS).aggregate([])
        for shard, bucket, origin in zip(
            partitions["shard"].to_pylist(),
            partitions["bucket"].to_pylist(),
            partitions["origin"].to_pylist(),
        ):
            self._tags.set(tag.statements_partition_updated(shard, bucket, origin))

    @property
    def needs_merge(self) -> bool:
        """Whether any partition has been written to since its last merge.

        Reads are canonical only on a merged store – the live ``statement``
        view does no read-time dedupe – so paths that publish canonical rows
        (`export_diff`)
        check this first.

        The dataset-level ``statements/last_optimized`` tag cannot answer it:
        [`OptimizeOperation`][ftm_lakehouse.operation.maintenance.OptimizeOperation] stamps
        that with its *start* time while [`merge`][ParquetStore.merge] bumps
        ``statements/last_updated`` on completion, so the dataset pair reads
        stale right after a successful optimize. The per-partition tags are
        the ones [`merge`][ParquetStore.merge] itself compares, stamped in the order that
        makes the comparison sound.
        """
        for shard, bucket, origin in self._list_partitions():
            updated = tag.statements_partition_updated(shard, bucket, origin)
            optimized = tag.statements_partition_optimized(shard, bucket, origin)
            if not self._tags.is_latest(optimized, [updated]):
                return True
        return False

    def merge(self, force: bool = False) -> None:
        """Collapse duplicates and reap expired tombstones, partition by partition.

        For each ``(shard, bucket, origin)`` partition, runs the merge
        query against ``statement_raw`` (non-fragment rows: keep latest
        row per ``id`` by ``last_seen DESC``; fragment rows: keep the
        latest emission per ``(entity_id, prop, fragment)`` group; fold
        ``first_seen`` to the min; drop tombstones older than the grace
        cutoff) and atomically overwrites that partition via
        ``partition_filters``. Held under the exclusive maintenance fence
        (``path.LOCK`` + append-marker drain, `_maintenance_fence`).

        Only partitions whose ``last_updated`` freshness tag is newer than
        their ``last_optimized`` tag are rewritten – a partition untouched
        since its last merge is skipped, so an optimize after a small
        append rewrites only what changed instead of the whole store. Each
        successful rewrite stamps ``last_optimized``.

        Because a clean partition is never revisited by a *default* merge,
        a tombstone sitting in an otherwise-idle partition is not
        physically reaped once it passes the grace window until the next
        write touches that partition – this only defers disk reclamation;
        read correctness is unaffected (the live view hides tombstones
        regardless). ``force=True`` bypasses the skip and re-evaluates
        every partition, so a forced merge (with
        ``LAKEHOUSE_GRACE_PERIOD_DAYS=0`` for an immediate purge)
        physically reaps cold tombstones too.

        Load-bearing for reads: the live ``statement`` view does no
        dedupe, so a partition's rows are only canonical – one row per id,
        fragment supersession applied, ``first_seen`` / ``last_seen``
        folded – after this runs. Reads assume every touched partition has
        been merged since its last write.

        A partition whose parquet size suggests the merge pipeline would
        outgrow ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``
        (`merge_slice_count`) is merged
        in contiguous ``entity_id`` range slices instead of one pass: a
        reservoir sample picks boundaries
        (`slice_ranges`), one merge
        query runs per range – strictly sequentially, so only one sort
        window is materialised at a time – and the slices chain into the
        single atomic partition overwrite (`_chained_reader`). No
        dedupe group spans an ``entity_id`` bound, ranges stream in
        ascending order, so output content, file sort order and the Delta
        commit are identical to a single-pass merge.

        Args:
            force: Rewrite every partition regardless of freshness tags.
        """
        if not self.exists:
            return
        grace_cutoff = utc_now() - timedelta(days=self.settings.grace_period_days)
        merged = skipped = 0
        with self._maintenance_fence():
            sizes = self._partition_bytes()
            for shard, bucket, origin in self._list_partitions():
                updated = tag.statements_partition_updated(shard, bucket, origin)
                optimized = tag.statements_partition_optimized(shard, bucket, origin)
                if not (force or not self._tags.is_latest(optimized, [updated])):
                    skipped += 1
                    continue
                with Took() as t, self._tags.touch(optimized):
                    slices = merge_slice_count(
                        sizes.get((shard, bucket, origin), 0),
                        self.settings.duckdb_memory_limit,
                    )
                    with self._lake.cursor() as cur:
                        ranges: list[tuple[str | None, str | None]] = [(None, None)]
                        if slices > 1:
                            sample_sql = build_bounds_sample_sql(shard, bucket, origin)
                            sample = [r[0] for r in cur.execute(sample_sql).fetchall()]
                            ranges = slice_ranges(sample, slices)
                        sqls = [
                            build_merge_sql(
                                shard, bucket, origin, grace_cutoff, entity_id_range=r
                            )
                            for r in ranges
                        ]
                        write_deltalake(
                            str(self.uri),
                            self._chained_reader(cur, sqls),
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
                        grace_period_days=self.settings.grace_period_days,
                        slices=len(ranges),
                    )
            if merged:
                # A rewrite changes the store's logical *canonical* content
                # (duplicates collapse, deletes apply), which is what every
                # downstream consumer reads - exports, statistics, diffs. They
                # key on STATEMENTS_OPTIMIZED, stamped here on completion, so
                # they go stale exactly when the canonical content moved.
                # STATEMENTS_UPDATED stays the append-side clock: it says rows
                # landed, not that they are canonical yet.
                self._tags.set(tag.STATEMENTS_OPTIMIZED)
        self.log.info(
            "Merge complete.",
            merged=merged,
            skipped=skipped,
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

        Both rewriting paths feed it: [`merge`][ParquetStore.merge] passes its range
        slices, which arrive in ascending ``entity_id`` order and are
        each internally sorted, so the concatenated stream keeps the
        global file sort order; [`shard`][ParquetStore.shard] passes one query per
        source partition, deliberately unordered.

        Args:
            cur: Open DuckDB cursor – must stay alive until the returned
                reader is fully consumed.
            sqls: Queries in output order; a single-pass merge passes
                exactly one.
        """
        first = cur.execute(sqls[0]).to_arrow_reader()

        def batches() -> Iterator[pa.RecordBatch]:
            yield from first
            for sql in sqls[1:]:
                yield from cur.execute(sql).to_arrow_reader()

        return pa.RecordBatchReader.from_batches(first.schema, batches())

    def _partition_bytes(self) -> dict[tuple[str, str, str], int]:
        """Physical parquet bytes per ``(shard, bucket, origin)`` partition.

        Summed from the Delta log's add actions – file-level metadata,
        no data scan. Drives the slice count of a range-sliced
        [`merge`][ParquetStore.merge].
        """
        actions = pa.table(self.deltatable.get_add_actions(flatten=True))
        sizes: dict[tuple[str, str, str], int] = {}
        for size, shard, bucket, origin in zip(
            actions["size_bytes"].to_pylist(),
            actions["partition.shard"].to_pylist(),
            actions["partition.bucket"].to_pylist(),
            actions["partition.origin"].to_pylist(),
        ):
            key = (shard, bucket, origin)
            sizes[key] = sizes.get(key, 0) + size
        return sizes

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
        rows rather than deciding which survive. Every rewritten
        partition is therefore re-stamped as dirty, so the next
        [`merge`][ParquetStore.merge] restores canonical content and file sort order –
        run ``optimize`` afterwards. The stamps are per-partition only;
        the dataset-level clocks stay put, because a re-shard changes
        physical layout, not canonical content, and the exports keyed on
        them are byte-identical either side of it.

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
        """Rewrite every ``(bucket, origin)`` group onto ``shards`` shards."""
        with self._maintenance_fence():
            groups: dict[tuple[str, str], list[str]] = {}
            for shard, bucket, origin in self._list_partitions():
                groups.setdefault((bucket, origin), []).append(shard)
            for (bucket, origin), sources in groups.items():
                sqls = [build_shard_sql(s, bucket, origin, shards) for s in sources]
                with Took() as t, self._lake.cursor() as cur:
                    write_deltalake(
                        str(self.uri),
                        self._chained_reader(cur, sqls),
                        mode="overwrite",
                        partition_by=PARTITIONS,
                        predicate=(
                            f"bucket = '{bucket}' AND "
                            f"origin = '{validate_origin(origin)}'"
                        ),
                        writer_properties=writer_for_bucket(bucket),
                        target_file_size=shard_target_file_size(shards),
                        storage_options=storage_options(),
                    )
                self.log.info(
                    f"Re-sharded `{bucket}/{origin}`.",
                    took=t.took,
                    bucket=bucket,
                    origin=origin,
                    sources=len(sources),
                    shards=shards,
                )
            for shard, bucket, origin in self._list_partitions():
                self._tags.set(tag.statements_partition_updated(shard, bucket, origin))

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
        [`STATEMENTS_OPTIMIZED`][ftm_lakehouse.core.conventions.tag.STATEMENTS_OPTIMIZED]
        on completion when rows were removed – dropping a partition moves the
        store's canonical content exactly as a merge does, so exports,
        statistics and diffs have to go stale against it. The append-side
        ``STATEMENTS_UPDATED`` clock is deliberately left alone: no rows
        landed. The dropped partitions' own tags are left behind too – they
        no longer enumerate, and a later write to the same origin stamps a
        fresh ``last_updated`` over the stale ``last_optimized``, so the
        partition comes back dirty.

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
        with self._maintenance_fence(), Took() as t:
            # safe to interpolate: `validate_origin` rejects quotes
            metrics = self.deltatable.delete(f"origin = '{origin}'")
            deleted = int(metrics.get("num_deleted_rows") or 0)
            if deleted:
                self._tags.set(tag.STATEMENTS_OPTIMIZED)
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
        with self._maintenance_fence(), Took() as t:
            self.deltatable.vacuum(
                retention_hours=retention_hours,
                dry_run=False,
                enforce_retention_duration=False,
            )
            self.log.info("Vacuumed.", took=t.took)

    def export_csv(self, key: str) -> None:
        """Export statements to a sorted CSV file.

        Streams each ``(shard, bucket)`` partition straight from DuckDB as
        Arrow batches (`_execute_partitioned`) into a ``pyarrow`` CSV
        writer, so the export stays vectorised end to end – no per-row
        Python materialisation. Memory stays bounded per batch and the
        ``ORDER BY entity_id`` sort stays bounded to one partition.

        Compression comes from `compression` (the dataset's config), not
        from the caller.
        """
        if not self.exists:
            return
        sql = statement_csv_select()
        with (
            self._store.open(key, "wb") as fh,
            compress_stream(fh, self.compression) as out,
        ):
            writer: CSVWriter | None = None
            for reader in self._execute_partitioned(sql):
                for batch in reader:
                    if writer is None:
                        writer = CSVWriter(out, batch.schema)
                    writer.write(batch)
            if writer is not None:
                writer.close()

    def get_entity_ids(
        self, q: Query | None = None, *, source: SqlSource | None = None
    ) -> Iterator[str]:
        """Get entity IDs for given query. Use ``self.source_raw`` to
        target physical storage without tombstones merged"""

        if not self.exists:
            return

        sql = Sql(q or Query(), source=source or self.source).canonical_ids
        for reader in self._execute_partitioned(sql):
            for batch in reader:
                yield from batch["entity_id"].to_pylist()

    def destroy(self) -> None:
        """
        Destroy the deltalake by removing the transaction log in "_delta_log"
        directory. This is soft deleting, as the parquet files remain (but will
        be cleaned up on optimize --vacuum)
        """
        with Took() as t:
            self.log.warn("🔥 Destroying deltalake store ...")
            prefix = f"{path.STATEMENTS}/_delta_log"
            for key in self._store.iterate_keys(prefix):
                self._store.delete(key)
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

    def _iter_shard_buckets(self) -> Iterator[tuple[str, str]]:
        """Yield unique ``(shard, bucket)`` pairs from existing partitions.

        Reads (`_query_statement_data`) iterate per ``(shard,
        bucket)`` because entity IDs (and thus statement IDs) are uniquely
        placed in one ``(shard, bucket)`` by the model layer. Adding
        ``WHERE shard = ? AND bucket = ?`` per iteration keeps a full-store
        ``ORDER BY entity_id`` bounded to one partition and lets the
        predicate push through the live view's plain scan to the parquet
        file statistics.
        """
        seen: set[tuple[str, str]] = set()
        for s, b, _ in self._list_partitions():
            key = (s, b)
            if key not in seen:
                seen.add(key)
                yield s, b

    def _scoped_partition_sql(self, sql: Select) -> Iterator[Select]:
        """Yield ``sql`` scoped with ``WHERE shard = ? AND bucket = ?`` per
        ``(shard, bucket)`` partition.

        The per-partition scoping keeps a full-store ``ORDER BY entity_id``
        bounded to one partition (an entity lives in one ``(shard, bucket)``)
        and lets every filter push through the live ``statement`` view's plain
        ``deleted_at IS NULL`` scan to ``delta_scan``'s per-file statistics.
        """
        for s, b in self._iter_shard_buckets():
            yield sql.where(column("shard") == s, column("bucket") == b)

    def _execute_partitioned(
        self, sql: Select | None = None
    ) -> Iterator[pa.RecordBatchReader]:
        """Yield a streamed Arrow reader per ``(shard, bucket)`` partition.

        Hands back each partition's result (scoped via
        `_scoped_partition_sql`) as a lazy
        `pyarrow.RecordBatchReader` streamed from DuckDB's execution
        pipeline, so memory stays bounded per batch instead of materialising
        the partition.

        Consume each reader fully before advancing to the next: the backing
        cursor is held open only across its ``yield`` and closes when the
        generator resumes for the following partition.

        Args:
            sql: Optional SQLAlchemy ``Select`` (default: `_compile_query`).

        Yields:
            One `pyarrow.RecordBatchReader` per ``(shard, bucket)``
            partition.
        """
        if sql is None:
            sql = self._compile_query()
        for scoped in self._scoped_partition_sql(sql):
            compiled = str(scoped.compile(compile_kwargs={"literal_binds": True}))
            with self._lake.cursor() as cur:
                yield cur.execute(compiled).to_arrow_reader()

    def _query_statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Query statement dicts from the live view, bypassing FtM construction.

        Iterates ``(shard, bucket)`` partitions via
        `_scoped_partition_sql`. Correctness assumes an optimized store –
        on an un-merged store this can surface duplicate ids and rows whose
        delete has not been applied yet.

        Args:
            q: Optional ftmq ``Query`` (default: match-all), compiled via
                `_compile_query`.

        Yields:
            StatementDict instances.
        """
        for scoped in self._scoped_partition_sql(self._compile_query(q)):
            for row in self._lake._execute(scoped):
                yield StatementDict(**vars(row))

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
