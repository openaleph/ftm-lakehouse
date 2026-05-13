"""ParquetStore – Delta Lake table with entity-hash shard partitioning.

Statements live in one Delta Lake table (per dataset) partitioned by
``(shard, bucket, origin)``. ``shard`` is the hex-padded entity_id hash bucket;
the uniform shard count is set per dataset via ``DatasetModel.shards``.

Writes are **append-only**: each flush sorts a per-partition batch by
``(entity_id, id, last_seen DESC)`` in memory and appends it as a new parquet
file. Deduplication, ``first_seen`` merging, and tombstone reaping are deferred
to the async ``merge`` operation. ``compact`` bin-packs small files; ``vacuum``
removes obsolete Delta file versions.

Layout:
    entities/statements/shard={s}/bucket={b}/origin={o}/part-*.parquet
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Iterator

import duckdb
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
    stream_duckdb,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from ftmq.util import make_dataset
from sqlalchemy import ColumnElement, Select, column, select

from ftm_lakehouse.core.api import LakehouseApiMixin, no_api
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.parquet import (
    build_merge_query,
    make_duckdb,
    register_view,
)
from ftm_lakehouse.model.statement import TABLE

PARTITIONS = ["shard", "bucket", "origin"]

VIEW_FILTER = column("deleted_at").is_(None)
"""SQLAlchemy filter appended to all queries via ftmq's view_filter mechanism."""


class ParquetStore(LakehouseApiMixin):
    """Single Delta Lake table (per dataset) partitioned by ``(shard, bucket,
    origin)``.

    Writes are append-only: ``append`` sorts a per-partition batch in memory
    and writes one parquet file. Reads delegate to an ftmq ``LakeStore`` with
    ``view_filter=deleted_at IS NULL`` (filters tombstones at query time;
    merge drops them physically once past grace).
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
            view_filter=VIEW_FILTER,
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

    @cached_property
    def _duckdb(self) -> duckdb.DuckDBPyConnection:
        """DuckDB connection with the Delta table registered as a view.

        ``register_view`` uses ``delta_scan`` so the view resolves the current
        Delta log on every query – registering once per store is enough; the
        view stays in sync with subsequent ``write_deltalake`` commits.

        DuckDB's :class:`~duckdb.DuckDBPyConnection` is *not* thread-safe;
        callers must not query this connection directly. Use
        :meth:`_cursor` to get a thread-isolated child connection that
        shares the catalog, the loaded Delta extension, and the
        registered view.
        """
        con = make_duckdb()
        register_view(con, self.deltatable)
        return con

    @contextmanager
    def _cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a thread-isolated DuckDB cursor.

        Per the DuckDB Python docs, ``DuckDBPyConnection.cursor()`` returns
        a separate connection sharing the underlying database (catalog,
        loaded extensions, registered views), which is the supported way
        to run concurrent queries from multiple threads against one
        ``ParquetStore``.

        Use as ``with self._cursor() as cur:`` for any synchronous query
        on the cached connection. Generators that need the cursor alive
        for streaming results should pin it in their closure so it isn't
        closed before consumption finishes.
        """
        cur = self._duckdb.cursor()
        try:
            yield cur
        finally:
            cur.close()

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

        Uses the shard partition for efficient pruning.
        """
        if not self.exists:
            return
        shard = path.entity_shard(entity_id, self.shards)
        q = select(TABLE).where(TABLE.c.shard == shard, TABLE.c.entity_id == entity_id)
        yield from self.query_statements(q)

    @no_api
    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store."""
        return self.view().stats()

    def _write_lock(self) -> Lock:
        """Dataset-wide write fence.

        All Delta writers (``append``, ``merge``, ``compact``, ``vacuum``)
        acquire this lock so they can't race on the same partition. The lock
        lives at ``{dataset_root}/.LOCK`` per ``path.LOCK``.
        """
        return Lock(self._store, key=path.LOCK)

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

        For each ``(shard, bucket, origin)`` partition, runs the merge query
        (keep latest row per ``id`` by ``last_seen DESC``; fold ``first_seen``
        to the min; drop tombstones older than the grace cutoff) and atomically
        overwrites that partition via ``partition_filters``. Held under the
        dataset write fence (``path.LOCK``).

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
                with self._cursor() as cur:
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
        the last diff state.
        """
        if not self.exists:
            return
        from sqlalchemy import or_

        since_truncated = since.replace(microsecond=0)
        sql = (
            select(TABLE)
            .distinct(TABLE.c.entity_id)
            .where(
                or_(
                    TABLE.c.first_seen >= since_truncated,
                    TABLE.c.deleted_at >= since_truncated,
                )
            )
        )
        if schemata:
            sql = sql.where(TABLE.c.schema.in_(schemata))
        if prop:
            sql = sql.where(TABLE.c.prop == prop)
        for shard in self._iter_shards():
            for row in stream_duckdb(sql.where(shard), self.deltatable):
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

    def _iter_shards(self) -> Iterator[ColumnElement]:
        """Get existing shard keys as Sqlalchemy predicates.

        Returns free-column predicates (not bound to ``TABLE``) so they can be
        added to queries built on ftmq's table object without dragging in a
        second same-named ``Table`` reference (which would yield
        ``FROM x, x`` in the rendered SQL).
        """
        shard_col = column("shard")
        q = select(shard_col).select_from(TABLE).distinct()
        for row in stream_duckdb(q, self.deltatable):
            yield shard_col == row.shard

    def _list_partitions(self) -> list[tuple[str, str, str]]:
        """List all ``(shard, bucket, origin)`` triples currently in the table."""
        if not self.exists:
            return []
        with self._cursor() as cur:
            rows = cur.execute(
                f"SELECT DISTINCT shard, bucket, origin FROM {TABLE.name} "
                "ORDER BY shard, bucket, origin"
            ).fetchall()
        return [(s, b, o) for s, b, o in rows]

    def _query_statement_data(self, q: Select | None = None) -> Iterator[StatementDict]:
        """
        Query statement dicts, bypassing FtM object construction.

        Args:
            q: Optional SQLAlchemy select (default: Query().sql.statements)

        Yields:
            StatementDict instances
        """
        if q is None:
            q = Query().sql.statements
        for shard in self._iter_shards():
            for row in stream_duckdb(q.where(shard), self.deltatable, VIEW_FILTER):
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
