"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Iterable

from anystore.store import get_store
from anystore.types import Uri
from followthemoney import Statement, StatementEntity
from ftmq.query import Query
from ftmq.types import StatementEntities
from ftmq.util import EntityProxy

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage import JournalStore, ParquetStore
from ftm_lakehouse.storage.journal import JournalWriter

settings = Settings()


class EntityRepository(BaseRepository):
    """
    Repository for entity/statement operations.

    Combines JournalStore (write-ahead buffer) and ParquetStore (Delta Lake)
    to provide buffered statement storage with efficient querying.

    Writes go to the journal first, then are flushed to the parquet store.
    Reads query the parquet store (optionally flushing first).

    Example:
        ```python
        repo = EntityRepository(uri="s3://bucket/dataset", dataset="my_data")

        # Write entities
        with repo.bulk(origin="import") as writer:
            writer.add_entity(entity)

        # Flush to parquet
        repo.flush()

        # Query entities
        for entity in repo.query(origin="import"):
            process(entity)
        ```
    """

    def __init__(
        self,
        dataset: str,
        uri: Uri,
        journal_uri: str | None = None,
    ) -> None:
        super().__init__(dataset, uri)
        self._journal = JournalStore(dataset, journal_uri or settings.journal_uri)
        self._statements = ParquetStore(uri, dataset)

    @contextmanager
    def bulk(self, origin: str | None = None) -> Generator[JournalWriter, None, None]:
        """
        Get a bulk writer for adding entities/statements.

        Usage:
            with repo.bulk(origin="import") as writer:
                writer.add_entity(entity)
        """
        writer = self._journal.writer(origin)
        try:
            yield writer
        except BaseException:
            writer.rollback()
            raise
        else:
            writer.flush()
        finally:
            writer.close()
            self._tags.set(tag.JOURNAL_UPDATED)

    def add(self, entity: EntityProxy, origin: str | None = None) -> None:
        """Add a single entity to the journal."""
        with self.bulk(origin) as writer:
            writer.add_entity(entity)

    def add_many(
        self, entities: Iterable[EntityProxy], origin: str | None = None
    ) -> None:
        """Add an entity iterator to the journal."""
        with self.bulk(origin) as writer:
            for entity in entities:
                writer.add_entity(entity)

    def flush(self) -> int:
        """
        Flush statements from journal to parquet store.

        Statements are streamed ordered by (bucket, origin, canonical_id).
        The parquet writer is flushed whenever bucket or origin changes.

        Returns:
            Number of statements flushed
        """
        total_count = 0
        current_bucket: str | None = None
        current_origin: str | None = None
        bulk = None

        for _, bucket, origin, _, data in self._journal.flush():
            # Flush and get new writer when partition changes
            if bucket != current_bucket or origin != current_origin:
                if bulk is not None:
                    bulk.flush()
                current_bucket = bucket
                current_origin = origin
                bulk = self._statements.writer(origin)

            assert bulk is not None
            stmt = unpack_statement(data)
            bulk.add_statement(stmt)
            total_count += 1

        # Flush final batch
        if bulk is not None:
            bulk.flush()

        if total_count > 0:
            self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Flushed statements from journal to lake",
                count=total_count,
            )

        return total_count

    def query(
        self,
        entity_ids: Iterable[str] | None = None,
        flush_first: bool = True,
        **filters,
    ) -> StatementEntities:
        """
        Query entities from the parquet store.

        Args:
            entity_ids: Filter by entity IDs
            flush_first: Flush journal before querying (default True)
            **filters: Additional query filter kwargs

        Yields:
            StatementEntity objects matching the query
        """
        if flush_first:
            self.flush()

        if entity_ids:
            filters["entity_id__in"] = list(entity_ids)
        q = Query().where(**filters)

        yield from self._statements.query(q)

    def get(
        self,
        entity_id: str,
        origin: str | None = None,
        flush_first: bool = True,
    ) -> StatementEntity | None:
        """Get a single entity by ID."""
        for entity in self.query([entity_id], flush_first, origin=origin):
            return entity
        return None

    def iterate(self) -> StatementEntities:
        """
        Stream entities from the exported JSON file.

        This reads from the pre-exported entities.ftm.json file,
        not directly from the parquet store.
        """
        from ftmq.io import smart_read_proxies

        store = get_store(self.uri)
        uri = store.get_key(path.ENTITIES_JSON)
        yield from smart_read_proxies(uri)

    def export_statements(self) -> None:
        """Export parquet store to sorted CSV file."""
        store = get_store(self.uri)
        store.ensure_parent(path.EXPORTS_STATEMENTS)
        output_uri = store.get_key(path.EXPORTS_STATEMENTS)
        self._statements.export_csv(output_uri)

    def export(self) -> None:
        """Export statements to entities.ftm.json file."""
        from ftmq.io import smart_write_proxies

        store = get_store(self.uri)
        store.ensure_parent(path.ENTITIES_JSON)
        output_uri = store.get_key(path.ENTITIES_JSON)
        smart_write_proxies(output_uri, self._statements.query())

    def get_statistics(self):
        """Compute statistics from the parquet store."""
        return self._statements.stats()

    def export_statistics(self, versions) -> None:
        """Export statistics to versioned JSON file."""
        stats = self.get_statistics()
        versions.make(path.STATISTICS, stats)

    def get_changes(
        self,
        start_version: int | None = None,
        end_version: int | None = None,
    ) -> Generator[tuple[datetime, str, Statement], None, None]:
        """Get statement changes for a version range."""
        yield from self._statements.get_changes(start_version, end_version)

    def optimize(self, vacuum: bool = False, vacuum_keep_hours: int = 0) -> None:
        """Optimize the parquet store."""
        self._statements.optimize(vacuum, vacuum_keep_hours)
