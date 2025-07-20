import contextlib
from datetime import datetime
from typing import Generator, Iterable

from anystore.util import join_uri
from followthemoney import Statement
from ftmq.query import Query
from ftmq.store.lake import (
    DEFAULT_ORIGIN,
    PARTITION_BY,
    LakeStore,
    LakeWriter,
    query_duckdb,
)
from ftmq.types import StatementEntities, StatementEntity

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.decorators import skip_if_latest
from ftm_lakehouse.lake.mixins import LakeMixin

PARTITIONS = [p for p in PARTITION_BY if p != "dataset"]


class DatasetStatements(LakeMixin):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._uri = join_uri(self.uri, path.STATEMENTS)
        self._store = LakeStore(
            uri=self._uri, dataset=self.name, partition_by=PARTITIONS
        )
        self.view = self._store.default_view()
        self.get_bulk = self._store.writer
        self.log.info("👋 DatasetStatements", store=self._uri)

    @contextlib.contextmanager
    def bulk(
        self, origin: str | None = DEFAULT_ORIGIN
    ) -> Generator[LakeWriter, None, None]:
        with self.tags.touch(tag.STATEMENTS_UPDATED):
            bulk = self.get_bulk(origin)
            try:
                yield bulk
            finally:
                bulk.flush()

    def iterate(
        self,
        entity_ids: Iterable[str] | None = None,
        origin: str | None = None,
        bucket: str | None = None,
    ) -> StatementEntities:
        q = Query()
        if entity_ids:
            q = q.where(entity_id__in=entity_ids)
        if origin:
            q = q.where(origin=origin)
        if bucket:
            q = q.where(bucket=bucket)
        yield from self.view.query(q)

    def get_entity(
        self,
        entity_id: str,
        origin: str | None = None,
        bucket: str | None = None,
    ) -> StatementEntity | None:
        for entity in self.iterate([entity_id], origin, bucket):
            return entity

    @skip_if_latest(path.STATISTICS, [tag.STATEMENTS_UPDATED])
    def export_statistics(self) -> None:
        """
        Compute statistics from the statement store and write it to versioned
        `statistics.json`. This could be used as a periodic task or after data
        changes.
        """
        stats = self.view.stats()
        self.versions.make(path.STATISTICS, stats)

    @skip_if_latest(path.EXPORTS_STATEMENTS, [tag.STATEMENTS_UPDATED])
    def export(self) -> None:
        """
        Sort, de-duplicate and export the statement store
        """
        self.storage.ensure_parent(path.EXPORTS_STATEMENTS)
        uri = self.storage.get_key(path.EXPORTS_STATEMENTS)
        db = query_duckdb(Query().sql.statements, self._store.deltatable)
        db.write_csv(uri)

    def get_changed_statements(
        self, start: int | None = None, end: int | None = None
    ) -> Generator[tuple[datetime, str, Statement], None, None]:
        """
        Get the added/changed statements for the given version range
        """
        while batch := self._store.deltatable.load_cdf(
            starting_version=start or 1, ending_version=end
        ).read_next_batch():
            for row in batch.to_struct_array().to_pylist():
                yield (
                    row["_commit_timestamp"],
                    row["_change_type"],
                    Statement.from_dict(row),
                )

    @skip_if_latest(tag.STORE_OPTIMIZED, [tag.STATEMENTS_UPDATED])
    def optimize(
        self, vacuum: bool | None = False, vacuum_keep_hours: int | None = 0
    ) -> None:
        writer = self._store.writer()
        writer.optimize(vacuum, vacuum_keep_hours)
