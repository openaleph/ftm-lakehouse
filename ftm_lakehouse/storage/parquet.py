"""ParquetStore – Delta Lake table with entity-hash shard partitioning.

Statements live in one Delta Lake table (per dataset) partitioned by
``(shard, bucket, origin)``. ``shard`` is the hex-padded entity_id hash bucket;
the uniform shard count is set per dataset via ``DatasetModel.shards``.

Writes are **append-only**: each flush sorts a per-partition batch by
``(entity_id, id, last_seen DESC)`` in memory and appends it as a new parquet
file. Read-time dedupe is baked into the underlying ``LakeStore`` connection
via two registered views – :func:`~ftm_lakehouse.logic.parquet.dedupe_view_sql`
produces the deduped ``statement`` view that every read targets, and
:func:`~ftm_lakehouse.logic.parquet.raw_view_sql` produces ``statement_raw``
for code paths that need tombstones and physical layout visible
(:meth:`merge`, :meth:`get_changed_entity_ids`).

Statement-level reads (:meth:`_query_statement_data` and everything that
funnels through it) iterate ``(shard, bucket)`` partitions in Python and
add ``WHERE shard = ? AND bucket = ?`` to each query so DuckDB's predicate
pushdown drives the deduped view's parquet scan to one partition's files
per iteration – the window function stays bounded to one parquet file's
worth of rows. ``stats()`` and ``view()`` go through the un-iterated
global view, which is fine because aggregations need a global view to
combine correctly across partitions.

``merge`` collapses physical duplicates and reaps tombstones past grace;
``compact`` bin-packs small files; ``vacuum`` removes obsolete Delta file
versions.

Layout:
    entities/statements/shard={s}/bucket={b}/origin={o}/part-*.parquet
"""

from datetime import datetime, timedelta, timezone
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc
from anystore.interface.lock import Lock
from anystore.io.write import smart_write_csv
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import Took, join_uri, mask_uri
from deltalake import DeltaTable, write_deltalake
from followthemoney import Statement, StatementEntity
from followthemoney.statement import StatementDict
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import (
    LakeQueryView,
    LakeStore,
    setup_duckdb_storage,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from ftmq.util import make_dataset
from sqlalchemy import Select, column, or_, select
from sqlalchemy.sql.elements import ColumnElement

from ftm_lakehouse.core.api import LakehouseApiMixin, no_api
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.parquet import (
    build_merge_query,
    dedupe_view_sql,
    duckdb_config,
    raw_view_sql,
)
from ftm_lakehouse.model.statement import TABLE, TABLE_RAW

PARTITIONS = ["shard", "bucket", "origin"]

# ftmq's ``view_filter`` is appended as a WHERE clause to every query the
# LakeStore runs. Applied to the raw-view stats store so ``stats()`` sees
# live rows only (tombstones stripped) without going through the dedupe
# window – fast aggregations on a freshly-merged table.
_LIVE_ROW_FILTER: ColumnElement = column("deleted_at").is_(None)


class ParquetStore(LakehouseApiMixin):
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: :meth:`append` sorts a per-partition batch in
    memory and writes one parquet file. Reads dedupe on the fly via the
    deduped ``statement`` view registered on the :class:`LakeStore`
    connection; :meth:`_query_statement_data` iterates ``(shard, bucket)``
    partitions so the window function stays bounded per iteration.
    :meth:`merge`, :meth:`compact`, :meth:`vacuum` provide physical
    cleanup but are no longer load-bearing for query correctness.
    """

    def __init__(self, uri: Uri, dataset: str, shards: int | None = None) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        super().__init__(self.uri)
        self.settings = Settings()
        self.dataset = dataset
        self.shards = shards if shards is not None else self.settings.entity_shards
        self._store = get_store(uri)
        self._lake = LakeStore(
            uri=str(self.uri),
            dataset=self.dataset,
            partition_by=PARTITIONS,
            view_sqls={
                TABLE.name: dedupe_view_sql,
                TABLE_RAW.name: raw_view_sql,
            },
            duckdb_config=duckdb_config(),
        )
        # Stats LakeStore registers the *raw* view as ``statement``.
        # Aggregations bypass the deduped view's window function – a
        # ~95× speedup on full-scan counts. ftmq's stats SQL uses
        # ``count(canonical_id.distinct())`` and similar distinct-key
        # aggregations, so physical duplicates from re-flushes don't
        # inflate entity counts: same ``canonical_id`` across N
        # physical rows still counts as one entity. The ``view_filter``
        # strips tombstone rows so deletions still show up correctly
        # post-flush, before merge has run.
        self._lake_stats = LakeStore(
            uri=str(self.uri),
            dataset=self.dataset,
            partition_by=PARTITIONS,
            view_sqls={TABLE.name: raw_view_sql},
            view_filter=_LIVE_ROW_FILTER,
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

    @no_api
    def query(self, q: Query | None = None) -> StatementEntities:
        """
        Query Entities from the store.

        Args:
            q: Optional Query object with filters

        Yields:
            StatementEntity objects matching the query
        """
        sql = (q or Query()).sql.statements
        for data in self._query_data(sql):
            yield data.to_entity()

    @no_api
    def query_statements(self, q: Select | None = None) -> Statements:
        """
        Query ordered Statements from the store.

        Args:
            q: Optional SQLAlchemy query (default: Query().sql.statements)

        Yields:
            Statement objects matching the query
        """
        for stmt_dict in self._query_statement_data(q):
            yield Statement.from_dict(stmt_dict)

    @no_api
    def get_statements(self, entity_id: str) -> Statements:
        """Query all live statements for a single entity.

        Scopes :meth:`_query_statement_data` iteration to the entity's
        own shard so single-entity lookups don't fan out to every
        ``(shard, bucket)`` pair.
        """
        if not self.exists:
            return
        shard = path.entity_shard(entity_id, self.shards)
        q = select(TABLE).where(TABLE.c.shard == shard, TABLE.c.entity_id == entity_id)
        for stmt_dict in self._query_statement_data(q, shard=shard):
            yield Statement.from_dict(stmt_dict)

    @no_api
    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store.

        Targets the raw-view stats :class:`LakeStore` (``statement`` =
        raw ``delta_scan`` with the ``deleted_at IS NULL`` filter) so
        ftmq's aggregation SQL doesn't pay the dedupe window's per-row
        overhead. ftmq's stats are distinct-keyed
        (``count(canonical_id.distinct())`` etc.), so physical
        duplicates from re-flushes don't inflate entity counts –
        results stay correct between merges.
        """
        return self._lake_stats.default_view().stats()

    def _write_lock(self) -> Lock:
        """Dataset-wide write fence.

        All Delta writers (``append``, ``merge``, ``compact``, ``vacuum``)
        acquire this lock so they can't race on the same partition. The lock
        lives at ``{dataset_root}/.LOCK`` per ``path.LOCK``.

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

    @no_api
    def unlock(self) -> bool:
        """Forcibly release the dataset write fence.

        Operator escape hatch for the case where a writer process died
        with the lock held (or an attacker held it on purpose). The lock
        is just a file at ``{dataset_root}/.LOCK``; this method deletes
        it.

        **Use sparingly** – breaking a lock that's still held by a live
        writer can corrupt a write in flight. Confirm no process is
        actively writing before running.

        Returns:
            ``True`` if a lock was released, ``False`` if no lock was
            held.
        """
        if not self._store.exists(path.LOCK):
            return False
        self._store.delete(path.LOCK)
        return True

    @no_api
    def append(self, batch: pa.Table) -> None:
        """Append a sorted batch of statements.

        The batch should be scoped to a single ``shard`` for write efficiency
        (one parquet file per ``(shard, bucket, origin)`` partition). The
        method sorts by ``(bucket, origin, entity_id, id, last_seen DESC)``
        then splits by ``bucket`` so each ``write_deltalake`` call uses the
        bucket-appropriate ``writer_properties`` (small vs. large profile).
        Duplicates land as separate rows and are reaped by :meth:`merge`.

        Held under the dataset write fence so concurrent :meth:`merge` /
        :meth:`compact` / :meth:`vacuum` can't tombstone an in-flight append.

        Args:
            batch: PyArrow table with the columns of
                :data:`ftm_lakehouse.model.statement.SHARDED_SCHEMA`. Rows
                should already be scoped to a single shard.
        """
        if len(batch) == 0:
            return

        batch = batch.sort_by(
            [
                ("bucket", "ascending"),
                ("origin", "ascending"),
                ("entity_id", "ascending"),
                ("id", "ascending"),
                ("last_seen", "descending"),
            ]
        )
        with self._write_lock():
            mode = "append" if self.exists else "overwrite"
            for bucket in pc.unique(batch["bucket"]).to_pylist():
                sub = batch.filter(pc.equal(batch["bucket"], bucket))
                write_deltalake(
                    str(self.uri),
                    sub,
                    partition_by=PARTITIONS,
                    mode=mode,
                    writer_properties=writer_for_bucket(bucket),
                    storage_options=storage_options(),
                )
                # After the first sub-batch, the table exists for subsequent buckets.
                mode = "append"

    @no_api
    def merge(self, grace_period_days: int | None = None) -> None:
        """Collapse duplicates and reap expired tombstones, partition by partition.

        For each ``(shard, bucket, origin)`` partition, runs the merge
        query against ``statement_raw`` (keep latest row per ``id`` by
        ``last_seen DESC``; fold ``first_seen`` to the min; drop
        tombstones older than the grace cutoff) and atomically
        overwrites that partition via ``partition_filters``. Held under
        the dataset write fence (``path.LOCK``).

        Physical cleanup only – the deduped read-time view already
        produces the right query results without ``merge`` having run,
        so this is purely about reclaiming disk space and reaping
        tombstones past the grace window.

        Args:
            grace_period_days: Override ``settings.grace_period_days``. Pass
                ``0`` to drop tombstones immediately.
        """
        if not self.exists:
            return
        days = (
            grace_period_days
            if grace_period_days is not None
            else self.settings.grace_period_days
        )
        grace_cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with self._write_lock():
            for shard, bucket, origin in self._list_partitions():
                merge_select = build_merge_query(shard, bucket, origin, grace_cutoff)
                sql = str(merge_select.compile(compile_kwargs={"literal_binds": True}))
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
                        storage_options=storage_options(),
                    )

    @no_api
    def compact(self) -> None:
        """Bin-pack small parquet files within each partition.

        Cheap maintenance – Delta's ``OPTIMIZE compact`` only rewrites small
        files into larger ones; it does not collapse duplicate rows or drop
        tombstones (use :meth:`merge` for that). Held under the dataset write
        fence (``path.LOCK``).
        """
        if not self.exists:
            return
        with self._write_lock():
            for shard, bucket, origin in self._list_partitions():
                self.deltatable.optimize.compact(
                    partition_filters=[
                        ("shard", "=", shard),
                        ("bucket", "=", bucket),
                        ("origin", "=", origin),
                    ],
                    writer_properties=writer_for_bucket(bucket),
                )

    @no_api
    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete obsolete parquet files no longer referenced by the Delta log.

        Tombstoned files (replaced by :meth:`merge` / :meth:`compact`) become
        orphans on disk; vacuum prunes them once they're past
        ``retention_hours``. Held under the dataset write fence
        (``path.LOCK``).

        Args:
            retention_hours: Keep files newer than this many hours. ``0``
                drops every file the Delta log no longer references.
        """
        if not self.exists:
            return
        with self._write_lock():
            self.deltatable.vacuum(
                retention_hours=retention_hours,
                dry_run=False,
                enforce_retention_duration=False,
            )

    @no_api
    def export_csv(self, key: str, q: Select | None = None) -> None:
        """Export statements to a sorted CSV file."""
        if not self.exists:
            return
        items = self._query_statement_data(q)
        with self._store.open(key, "w") as f:
            smart_write_csv(f, items)

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
        Delta partitions directly without going through the deduped
        view's window function.
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

        Dedupe-aware reads (:meth:`_query_statement_data`) iterate per
        ``(shard, bucket)`` because entity IDs (and thus statement IDs)
        are uniquely placed in one ``(shard, bucket)`` by the model
        layer. Adding ``WHERE shard = ? AND bucket = ?`` to each
        iteration pushes through DuckDB's predicate pushdown to the
        deduped view's parquet scan, keeping the window function input
        bounded to one parquet file's worth of rows.

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

    def _query_statement_data(
        self, q: Select | None = None, *, shard: str | None = None
    ) -> Iterator[StatementDict]:
        """Query statement dicts via dedupe-on-read, bypassing FtM construction.

        Iterates over ``(shard, bucket)`` partitions, adding ``WHERE
        shard = ? AND bucket = ?`` to each query so the deduped
        ``statement`` view's window function operates on one partition
        at a time (DuckDB pushes the predicates through to the parquet
        scan's File Filters). The live view is correct without running
        :meth:`merge`: each statement id surfaces at most once,
        carrying the earliest ``first_seen`` and the latest
        ``last_seen``; tombstones are filtered out post-dedupe so a
        re-add of a deleted entity still surfaces.

        Args:
            q: Optional SQLAlchemy select (default:
                ``Query().sql.statements``).
            shard: Optional shard filter passed through to
                :meth:`_iter_shard_buckets` to scope iteration to one
                shard – used by single-entity lookups.

        Yields:
            StatementDict instances.
        """
        if q is None:
            q = Query().sql.statements
        for s, b in self._iter_shard_buckets(shard=shard):
            scoped = q.where(column("shard") == s, column("bucket") == b)
            for row in self._lake._execute(scoped):
                yield StatementDict(**vars(row))

    def _query_data(self, q: Select | None = None) -> Iterator[EntityPayload]:
        """
        Query entity dicts via aggregate_unsafe(), bypassing FtM object construction.

        Args:
            q: Optional SQLAlchemy select (default: Query().sql.statements)

        Yields:
            EntityPayload instances
        """
        if not self.exists:
            return
        yield from aggregate_unsafe(self._query_statement_data(q), self.dataset)
