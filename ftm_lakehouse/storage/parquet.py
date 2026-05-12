"""ParquetStore — Delta Lake table with entity-hash shard partitioning.

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

from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Iterator

import duckdb
import pyarrow as pa
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
        Delta log on every query — registering once per store is enough; the
        view stays in sync with subsequent ``write_deltalake`` commits.
        """
        con = make_duckdb()
        register_view(con, self.deltatable)
        return con

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

    @no_api
    def append(self, batch: pa.Table, bucket: str) -> None:
        """Append a sorted batch of statements to one partition.

        The batch should already be scoped to a single ``(shard, bucket,
        origin)`` triple; this method sorts it by
        ``(entity_id, id, last_seen DESC)`` and appends one parquet file.
        Duplicates land as separate rows and are reaped by ``merge``.
        """
        if len(batch) == 0:
            return

        batch = batch.sort_by(
            [
                ("entity_id", "ascending"),
                ("id", "ascending"),
                ("last_seen", "descending"),
            ]
        )
        mode = "append" if self.exists else "overwrite"
        write_deltalake(
            str(self.uri),
            batch,
            partition_by=PARTITIONS,
            mode=mode,
            writer_properties=writer_for_bucket(bucket),
            storage_options=storage_options(),
        )

    @no_api
    def merge(self, grace_period_days: int | None = None) -> None:
        """Collapse duplicates and reap expired tombstones, partition by partition.

        For each ``(shard, bucket, origin)`` partition, runs the merge query
        (keep latest row per ``id`` by ``last_seen DESC``; fold ``first_seen``
        to the min; drop tombstones older than the grace cutoff) and atomically
        overwrites that partition via ``partition_filters``. Held under
        ``Lock(path.lock("merge"))`` for the whole dataset.

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
        with Lock(self._store, key=path.lock("merge")):
            con = self._duckdb
            for shard, bucket, origin in self._list_partitions():
                merge_select = build_merge_query(shard, bucket, origin, grace_cutoff)
                sql = str(merge_select.compile(compile_kwargs={"literal_binds": True}))
                reader = con.execute(sql).fetch_record_batch()
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

        Cheap maintenance — Delta's ``OPTIMIZE compact`` only rewrites small
        files into larger ones; it does not collapse duplicate rows or drop
        tombstones (use ``merge`` for that). Held under
        ``Lock(path.lock("compact"))`` for the whole dataset.
        """
        if not self.exists:
            return
        with Lock(self._store, key=path.lock("compact")):
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

        Tombstoned files (replaced by ``merge`` / ``compact``) become orphans
        on disk; vacuum prunes them once they're past ``retention_hours``.
        Held under ``Lock(path.lock("vacuum"))``.
        """
        if not self.exists:
            return
        with Lock(self._store, key=path.lock("vacuum")):
            self.deltatable.vacuum(
                retention_hours=retention_hours,
                dry_run=False,
                enforce_retention_duration=False,
            )

    @no_api
    def export_csv(self, uri: Uri, q: Select | None = None) -> None:
        """Export statements to a sorted CSV file."""
        if not self.exists:
            return
        items = self._query_statement_data(q)
        with self._store.open(uri, "w") as f:
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
        and *deleted* ones (``deleted_at >= since``) — the latter so the diff
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
        rows = self._duckdb.execute(
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
