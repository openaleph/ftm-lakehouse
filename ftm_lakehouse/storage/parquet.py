"""ParquetStore - Delta Lake statement parquet storage."""

from datetime import datetime
from typing import Generator

from anystore.types import Uri
from anystore.util import join_uri
from followthemoney import Statement
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.store.lake import (
    LakeQueryView,
    LakeStore,
    LakeWriter,
    query_duckdb,
)
from ftmq.types import StatementEntities

from ftm_lakehouse.core.conventions import path

# Use same partitions as ftmq but exclude dataset (handled at directory level)
PARTITIONS = ["bucket", "origin"]


class ParquetStore:
    """
    Delta Lake parquet storage for entity statements.

    Wraps ftmq's LakeStore to provide statement storage with:
    - Partitioned parquet files (by bucket, origin)
    - Delta Lake transaction log for versioning
    - Change data capture (CDC) support
    - Efficient querying via DuckDB

    Layout: statements/bucket={bucket}/origin={origin}/{auto-identifier}.parquet
    """

    def __init__(self, uri: Uri, dataset: str) -> None:
        self.uri = join_uri(uri, path.STATEMENTS)
        self.dataset = dataset
        self._store = LakeStore(
            uri=self.uri,
            dataset=dataset,
            partition_by=PARTITIONS,
        )

    def writer(self, origin: str | None = None) -> LakeWriter:
        """Get a writer for adding statements."""
        return self._store.writer(origin)

    def view(self) -> LakeQueryView:
        """Get a view for querying statements."""
        return self._store.default_view()

    def query(self, q: Query | None = None) -> StatementEntities:
        """
        Query statements from the store.

        Args:
            q: Optional Query object with filters

        Yields:
            StatementEntity objects matching the query
        """
        view = self.view()
        yield from view.query(q or Query())

    def stats(self) -> DatasetStats:
        """Compute statistics from the statement store."""
        return self.view().stats()

    def export_csv(self, output_uri: str) -> None:
        """
        Export statements to a sorted, de-duplicated CSV file.

        Args:
            output_uri: Destination URI for the CSV file
        """
        db = query_duckdb(Query().sql.statements, self._store.deltatable)
        db.write_csv(output_uri)

    def get_changes(
        self,
        start_version: int | None = None,
        end_version: int | None = None,
    ) -> Generator[tuple[datetime, str, Statement], None, None]:
        """
        Get statement changes for a version range using change data capture.

        Args:
            start_version: Starting version number (default: 1)
            end_version: Ending version number (default: latest)

        Yields:
            Tuples of (commit_timestamp, change_type, statement)
        """
        while batch := self._store.deltatable.load_cdf(
            starting_version=start_version or 1,
            ending_version=end_version,
        ).read_next_batch():
            for row in batch.to_struct_array().to_pylist():
                yield (
                    row["_commit_timestamp"],
                    row["_change_type"],
                    Statement.from_dict(row),
                )

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

    @property
    def version(self) -> int:
        """Current version of the Delta table."""
        return self._store.deltatable.version()
