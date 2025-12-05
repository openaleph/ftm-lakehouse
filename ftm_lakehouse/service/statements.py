"""
Statement storage layer using Delta Lake.

Handles reading, querying, and exporting statements from the lake store.
This is an internal module - client applications should use DatasetEntities.
"""

import contextlib
from datetime import datetime
from functools import wraps
from typing import Callable, Generator, Iterable, TypeVar

from anystore.util import join_uri
from followthemoney import Statement
from ftmq.query import Query
from ftmq.store.lake import (
    PARTITION_BY,
    LakeStore,
    LakeWriter,
    query_duckdb,
)
from ftmq.types import StatementEntities, StatementEntity

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.core.decorators import skip_if_latest
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.service.journal import Journal, JournalWriter

PARTITIONS = [p for p in PARTITION_BY if p != "dataset"]

T = TypeVar("T")


def flush_journal_first(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator that flushes the journal before executing the method.

    Use this on methods that need to read from the statement store to ensure
    all pending writes are visible.
    """

    @wraps(func)
    def wrapper(self: "StatementStore", *args, **kwargs) -> T:
        self.flush_journal()
        return func(self, *args, **kwargs)

    return wrapper


class StatementStore(LakeMixin):
    """
    Internal statement storage layer.

    Manages the Delta Lake statement store and SQL journal for buffered writes.
    Client applications should use DatasetEntities instead.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._uri = join_uri(self.uri, path.STATEMENTS)
        self._store = LakeStore(
            uri=self._uri, dataset=self.name, partition_by=PARTITIONS
        )
        self._journal = Journal(
            name=self.name, uri=self.uri, journal_uri=self.settings.journal_uri
        )
        self.view = self._store.default_view()
        self._get_bulk = self._store.writer

    @contextlib.contextmanager
    def bulk(self, origin: str | None = None) -> Generator[JournalWriter, None, None]:
        """Get a bulk writer for adding statements to the journal."""
        with self.tags.touch(tag.JOURNAL_UPDATED):
            with self._journal.writer(origin=origin) as writer:
                yield writer

    @flush_journal_first
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

    @flush_journal_first
    def get(
        self,
        entity_id: str,
        origin: str | None = None,
        bucket: str | None = None,
    ) -> StatementEntity | None:
        for entity in self.iterate([entity_id], origin, bucket):
            return entity
        return None

    @flush_journal_first
    @skip_if_latest(path.STATISTICS, [tag.STATEMENTS_UPDATED])
    def export_statistics(self) -> None:
        """
        Compute statistics from the statement store and write it to versioned
        `statistics.json`.
        """
        stats = self.view.stats()
        self.versions.make(path.STATISTICS, stats)

    @flush_journal_first
    @skip_if_latest(path.EXPORTS_STATEMENTS, [tag.STATEMENTS_UPDATED])
    def export_statements(self) -> None:
        """
        Sort, de-duplicate and export the statement store to CSV.
        """
        self.storage.ensure_parent(path.EXPORTS_STATEMENTS)
        uri = self.storage.get_key(path.EXPORTS_STATEMENTS)
        db = query_duckdb(Query().sql.statements, self._store.deltatable)
        db.write_csv(uri)

    @flush_journal_first
    def get_changed_statements(
        self, start: int | None = None, end: int | None = None
    ) -> Generator[tuple[datetime, str, Statement], None, None]:
        """
        Get the added/changed statements for the given version range.
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

    @skip_if_latest(tag.STORE_OPTIMIZED, [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED])
    @flush_journal_first
    def optimize(
        self, vacuum: bool | None = False, vacuum_keep_hours: int | None = 0
    ) -> None:
        writer = self._store.writer()
        writer.optimize(vacuum, vacuum_keep_hours)

    @skip_if_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED])
    def flush_journal(self) -> int:
        """
        Flush statements from the journal to the lake storage.

        Statements are streamed ordered by (bucket, origin, canonical_id).
        The lake writer is flushed whenever bucket or origin changes to respect
        the lake's partitioning scheme.

        Returns:
            Number of statements flushed
        """
        total_count = 0
        current_bucket: str | None = None
        current_origin: str | None = None
        bulk: LakeWriter | None = None

        for bucket, origin, stmt in self._journal.flush():
            # Flush and get new writer when partition changes
            if bucket != current_bucket or origin != current_origin:
                if bulk is not None:
                    bulk.flush()
                current_bucket = bucket
                current_origin = origin
                bulk = self._get_bulk(origin)

            assert bulk is not None  # guaranteed by the condition above
            bulk.add_statement(stmt)
            total_count += 1

        # Flush final batch
        if bulk is not None:
            bulk.flush()

        if total_count > 0:
            with self.tags.touch(tag.STATEMENTS_UPDATED):
                pass
            self.log.info(
                "Flushed statements from journal to lake",
                count=total_count,
            )

        return total_count
