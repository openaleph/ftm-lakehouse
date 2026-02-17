"""ParquetStore - Delta Lake statement parquet storage with translog metadata."""

from datetime import datetime
from typing import Any, Generator, Iterator

import pyarrow as pa
from anystore.logging import get_logger
from anystore.types import Uri
from anystore.util import Took, join_uri, mask_uri
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import TABLE as _TABLE
from ftmq.store.lake import (
    LakeQueryView,
    LakeStore,
    LakeWriter,
    query_duckdb,
    setup_duckdb_storage,
    storage_options,
    stream_duckdb,
)
from ftmq.types import StatementEntities, Statements
from sqlalchemy import Select

from ftm_lakehouse.core.api import LakehouseApiMixin, no_api
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.entities import aggregate_unsafe
from ftm_lakehouse.logic.parquet import (
    compact_with_translog,
    filter_live_translog,
)
from ftm_lakehouse.logic.parquet import (
    get_deleted_entity_ids as _get_deleted_entity_ids,
)
from ftm_lakehouse.logic.parquet import (
    query_duckdb_translog,
    stream_duckdb_translog,
)

# Use same partitions as ftmq but exclude dataset (handled at directory level)
PARTITIONS = ["bucket", "origin"]

TRANSLOG_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("first_seen", pa.timestamp("us")),
        pa.field("last_seen", pa.timestamp("us")),
        pa.field("deleted_at", pa.timestamp("us")),
    ]
)


class TranslogStore(LakehouseApiMixin):
    """Manages a lightweight translog Delta table for per-statement metadata.

    Tracks first_seen, last_seen, and deleted_at per statement ID.
    The main parquet table stores immutable FtM statements; the translog
    provides mutable metadata via Delta Lake MERGE operations.
    """

    def __init__(self, uri: Uri, dataset: str) -> None:
        self.uri = join_uri(uri, path.TRANSLOG)
        super().__init__(self.uri)
        self.dataset = dataset
        setup_duckdb_storage()

    @property
    def deltatable(self) -> DeltaTable:
        return DeltaTable(str(self.uri), storage_options=storage_options())

    @property
    def exists(self) -> bool:
        try:
            self.deltatable.version()
            return True
        except TableNotFoundError:
            return False

    @no_api
    def upsert(self, table: pa.Table) -> None:
        """Insert or update translog rows. Updates last_seen on conflict."""
        if not self.exists:
            write_deltalake(
                str(self.uri),
                table,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=storage_options(),
            )
            return
        (
            self.deltatable.merge(
                source=table,
                predicate="target.id = source.id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(
                {
                    "last_seen": "source.last_seen",
                }
            )
            .when_not_matched_insert_all()
            .execute()
        )

    @no_api
    def mark_deleted(self, table: pa.Table) -> None:
        """Set deleted_at on existing translog rows.

        Args:
            table: PyArrow table with columns (id, deleted_at)
        """
        if not self.exists:
            return
        (
            self.deltatable.merge(
                source=table,
                predicate="target.id = source.id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(
                {
                    "deleted_at": "source.deleted_at",
                }
            )
            .execute()
        )

    @no_api
    def compact(self) -> None:
        """Remove deleted entries from translog."""
        if not self.exists:
            return

        live = filter_live_translog(self.deltatable)
        write_deltalake(
            str(self.uri),
            live,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_options(),
        )


class TranslogAwareLakeStore(LakeStore, LakehouseApiMixin):
    """LakeStore subclass that joins with translog for timestamps and soft deletes.

    All queries join the main table with the translog to get accurate
    first_seen/last_seen and filter deleted rows (deleted_at IS NOT NULL).
    Falls back to standard stream_duckdb when translog doesn't exist yet.
    """

    def __init__(self, *args, translog: TranslogStore, **kwargs) -> None:
        LakeStore.__init__(self, *args, **kwargs)
        LakehouseApiMixin.__init__(self, self.uri)
        self._translog = translog

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        try:
            dt = self.deltatable
        except TableNotFoundError:
            return
        if not self._translog.exists:
            yield from stream_duckdb(q, dt)
            return
        yield from stream_duckdb_translog(q, dt, self._translog.deltatable)


class ParquetStore(LakehouseApiMixin):
    """
    Delta Lake parquet storage for entity statements.

    Wraps ftmq's LakeStore to provide statement storage with:
    - Partitioned parquet files (by bucket, origin)
    - Delta Lake transaction log for versioning
    - Translog metadata table for timestamps and soft deletes
    - Change data capture (CDC) support
    - Efficient querying via DuckDB

    Layout: statements/bucket={bucket}/origin={origin}/{auto-identifier}.parquet
    """

    TABLE = _TABLE

    def __init__(self, uri: Uri, dataset: str) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        super().__init__(self.uri)
        self.dataset = dataset
        self._translog = TranslogStore(uri, dataset)
        self._store = TranslogAwareLakeStore(
            uri=self.uri,
            dataset=dataset,
            partition_by=PARTITIONS,
            translog=self._translog,
        )
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            uri=mask_uri(self.uri),
        )
        setup_duckdb_storage()

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        if self._store.exists:
            return self._store.deltatable.version()

    @property
    def translog_version(self) -> int | None:
        """Current version of the translog Delta table."""
        if not self._translog.exists:
            return None
        return self._translog.deltatable.version()

    @property
    def exists(self) -> bool:
        """Check existence of deltatable"""
        return self._store.exists

    @no_api
    def writer(self, origin: str | None = None) -> LakeWriter:
        """Get a writer for adding statements."""
        return self._store.writer(origin)

    @no_api
    def view(self) -> LakeQueryView:
        """Get a view for querying statements."""
        return self._store.default_view()

    @no_api
    def query(self, q: Query | None = None) -> StatementEntities:
        """
        Query Entities from the store.

        Args:
            q: Optional Query object with filters

        Yields:
            StatementEntity objects matching the query
        """
        view = self.view()
        yield from view.query(q or Query())

    @no_api
    def query_statements(self, q: Select | None = None) -> Statements:
        """
        Query ordered Statements from the store.

        Args:
            q: Optional SQLAlchemy query (default: Query().sql.statements)

        Yields:
            Statement objects matching the query
        """
        view = self.view()
        yield from view.store._iterate_stmts(
            q if q is not None else Query().sql.statements
        )

    @no_api
    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store."""
        return self.view().stats()

    @no_api
    def export_csv(self, output_uri: str) -> None:
        """
        Export statements to a sorted, de-duplicated CSV file.

        Args:
            output_uri: Destination URI for the CSV file
        """
        self._store._backend.ensure_parent(output_uri)
        dt = self._store.deltatable
        q = Query().sql.statements
        if self._translog.exists:
            db, _ = query_duckdb_translog(q, dt, self._translog.deltatable)
        else:
            db = query_duckdb(q, dt)
        db.write_csv(output_uri)

    @no_api
    def query_raw(self, q: Select | None = None) -> Iterator[dict[str, Any]]:
        """
        Query entity dicts via aggregate_unsafe(), bypassing FtM object construction.

        Args:
            q: Optional SQLAlchemy select (default: Query().sql.statements)

        Yields:
            Entity dicts (id, schema, properties, caption, ...)
        """
        if not self.exists:
            return
        dt = self._store.deltatable
        if q is None:
            q = Query().sql.statements

        if self._translog.exists:
            rel, con = query_duckdb_translog(q, dt, self._translog.deltatable)
        else:
            rel = query_duckdb(q, dt)
            con = None  # noqa: F841 — prevent GC of translog connection

        columns = rel.columns
        yield from aggregate_unsafe(
            dict(zip(columns, row))
            for batch in iter(lambda: rel.fetchmany(100_000), [])
            for row in batch
        )

    @no_api
    def compact(self) -> None:
        """Apply translog to main table: remove deleted rows, update timestamps.

        After compact the main table is self-contained (accurate first_seen/
        last_seen, no deleted rows) and the translog only contains live entries.
        Caller should call optimize() afterwards for file compaction.
        """
        if not self._translog.exists:
            return

        live = compact_with_translog(self._store.deltatable, self._translog.deltatable)

        write_deltalake(
            str(self.uri),
            live,
            partition_by=PARTITIONS,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_options(),
            configuration={"delta.enableChangeDataFeed": "true"},
        )

        self._translog.compact()

    @no_api
    def get_deleted_entity_ids(self) -> set[str]:
        """Get entity IDs that have been soft-deleted via translog."""
        if not self._translog.exists:
            return set()

        return _get_deleted_entity_ids(
            self._store.deltatable, self._translog.deltatable
        )

    @no_api
    def get_changes(
        self,
        start_version: int | None = None,
        end_version: int | None = None,
    ) -> Generator[tuple[datetime, str, dict], None, None]:
        """
        Get statement changes for a version range using change data capture.

        Args:
            start_version: Starting version number (default: 0)
            end_version: Ending version number (default: latest)

        Yields:
            Tuples of (commit_timestamp, change_type, row_dict)
        """
        reader = self._store.deltatable.load_cdf(
            starting_version=start_version or 0,
            ending_version=end_version,
        )
        try:
            while batch := reader.read_next_batch():
                for row in batch.to_struct_array().to_pylist():
                    yield (
                        row["_commit_timestamp"],
                        row["_change_type"],
                        row,
                    )
        except StopIteration:
            return

    @no_api
    def optimize(
        self,
        vacuum: bool = False,
        vacuum_keep_hours: int = 0,
        bucket: str | None = None,
        origin: str | None = None,
    ) -> None:
        """
        Optimize the store by compacting small files.

        Args:
            vacuum: Also delete old file versions
            vacuum_keep_hours: Hours of history to retain when vacuuming
            bucket: Filter optimization to specific bucket partition
            origin: Filter optimization to specific origin partition
        """
        writer = self._store.writer()
        writer.optimize(vacuum, vacuum_keep_hours, bucket=bucket, origin=origin)

    @no_api
    def destroy(self) -> None:
        """
        Destroy the deltalake by removing the transaction log in "_delta_log"
        directory. This is soft deleting, as the parquet files remain (but will
        be cleaned up on optimize --vacuum)
        """
        with Took() as t:
            self.log.warn("🔥 Destroying deltalake store ...")
            for key in self._store._backend.iterate_keys("_delta_log"):
                self._store._backend.delete(key)
        self.log.info("Deleted statement store.", took=t.took)
