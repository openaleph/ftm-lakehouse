"""ParquetStore - Delta Lake statement parquet storage."""

from datetime import datetime
from typing import Any, Generator

import duckdb
import pyarrow as pa
from anystore.logging import get_logger
from anystore.types import Uri
from anystore.util import Took, join_uri
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import (
    ARROW_SCHEMA,
)
from ftmq.store.lake import TABLE as _TABLE
from ftmq.store.lake import (
    LakeQueryView,
    LakeStore,
    LakeWriter,
    Row,
    compile_query,
    setup_duckdb_storage,
    stream_duckdb,
)
from ftmq.types import StatementEntities, Statements
from sqlalchemy import Select

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.parquet import compact as _compact

# Use same partitions as ftmq but exclude dataset (handled at directory level)
PARTITIONS = ["bucket", "origin"]

# ARROW_SCHEMA extended with deleted_at — always written so the column is always present
STATEMENT_SCHEMA = ARROW_SCHEMA.append(pa.field("deleted_at", pa.timestamp("us")))


def _tombstone_aware_sql(compiled_query: str, dt: DeltaTable) -> str:
    """Wrap a compiled SQL query with CTEs that filter tombstoned rows.

    The CTE strategy:
    - __tombstoned: collects max(deleted_at) per statement id from tombstone rows
    - __live: keeps rows where deleted_at IS NULL and either no tombstone exists
      or last_seen > max(deleted_at) (handles re-add after delete)
    - original query runs against __live instead of arrow/statement
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    # Qualify with arrow. to avoid ambiguous refs after LEFT JOIN
    live_cols = ", ".join(f"arrow.{c}" for c in all_cols if c != "deleted_at")

    cte = f"""WITH __tombstoned AS (
    SELECT id, MAX(deleted_at) AS __max_del
    FROM arrow WHERE deleted_at IS NOT NULL GROUP BY id
),
__live AS (
    SELECT {live_cols}
    FROM arrow
    LEFT JOIN __tombstoned ON arrow.id = __tombstoned.id
    WHERE arrow.deleted_at IS NULL
    AND (__tombstoned.id IS NULL OR arrow.last_seen > __tombstoned.__max_del)
)
"""
    # Replace FROM clause: "FROM arrow as statement" or "FROM statement"
    rewritten = compiled_query.replace(
        "FROM arrow as statement", "FROM __live as statement"
    )
    return cte + rewritten


def _stream_duckdb_tombstone_aware(
    q: Select, dt: DeltaTable
) -> Generator[Any, None, None]:
    """Like stream_duckdb but filters tombstoned rows via CTE."""
    rel = duckdb.arrow(dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = _tombstone_aware_sql(compiled, dt)
    res = rel.query("arrow", sql)
    while rows := res.fetchmany(100_000):
        for row in rows:
            yield Row(dict(zip(res.columns, row)))


def _query_duckdb_tombstone_aware(q: Select, dt: DeltaTable) -> duckdb.DuckDBPyRelation:
    """Like query_duckdb but filters tombstoned rows via CTE."""
    rel = duckdb.arrow(dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = _tombstone_aware_sql(compiled, dt)
    return rel.query("arrow", sql)


class TombstoneAwareLakeStore(LakeStore):
    """LakeStore subclass that filters tombstoned rows from all queries.

    Queries are wrapped with CTEs to exclude deleted rows. The deleted_at
    column is always present in tables written by the current code. Falls
    back to standard stream_duckdb for legacy tables without the column.
    """

    def _execute(self, q: Select, stream: bool = True) -> Generator[Any, None, None]:
        try:
            dt = self.deltatable
        except TableNotFoundError:
            return
        if "deleted_at" not in {f.name for f in dt.schema().to_arrow()}:
            yield from stream_duckdb(q, dt)
            return
        yield from _stream_duckdb_tombstone_aware(q, dt)


class ParquetStore:
    """
    Delta Lake parquet storage for entity statements.

    Wraps ftmq's LakeStore to provide statement storage with:
    - Partitioned parquet files (by bucket, origin)
    - Delta Lake transaction log for versioning
    - Change data capture (CDC) support
    - Efficient querying via DuckDB
    - Tombstone-based soft deletes (materialized on compact)

    Layout: statements/bucket={bucket}/origin={origin}/{auto-identifier}.parquet
    """

    TABLE = _TABLE

    def __init__(self, uri: Uri, dataset: str) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        self.dataset = dataset
        self._store = TombstoneAwareLakeStore(
            uri=self.uri,
            dataset=dataset,
            partition_by=PARTITIONS,
        )
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            uri=self.uri,
        )
        setup_duckdb_storage()

    @property
    def version(self) -> int | None:
        """Current version of the Delta table."""
        try:
            return self._store.deltatable.version()
        except TableNotFoundError:
            # deltatable doesn't exist
            return

    @property
    def exists(self) -> bool:
        """Check existence of deltatable"""
        return self.version is not None

    def writer(self, origin: str | None = None) -> LakeWriter:
        """Get a writer for adding statements."""
        return self._store.writer(origin)

    def view(self) -> LakeQueryView:
        """Get a view for querying statements."""
        return self._store.default_view()

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

    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store."""
        return self.view().stats()

    def export_csv(self, output_uri: str) -> None:
        """
        Export statements to a sorted, de-duplicated CSV file.

        Args:
            output_uri: Destination URI for the CSV file
        """
        self._store._backend.ensure_parent(output_uri)
        dt = self._store.deltatable
        q = Query().sql.statements
        db = _query_duckdb_tombstone_aware(q, dt)
        db.write_csv(output_uri)

    def compact(self) -> None:
        """Dedup, remove tombstones, rewrite, callers should call optimize
        afterwards."""
        _compact(self._store.deltatable, PARTITIONS)

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
