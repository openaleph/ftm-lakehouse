"""
Unified entity interface for the FTM Lakehouse.

DatasetEntities is the primary interface for client applications to work with
entities. It provides high-level operations for reading, writing, and querying
entities while internally managing the statement storage layer.
"""

import contextlib
import csv
import sys
from datetime import datetime
from functools import cached_property
from typing import BinaryIO, Generator, Iterable, cast

from anystore.exceptions import DoesNotExist
from followthemoney import EntityProxy, Statement, StatementEntity
from followthemoney.statement.serialize import read_csv_statements
from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.model import DatasetStats
from ftmq.types import StatementEntities, ValueEntities

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.core.decorators import skip_if_latest
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.logic.entities import aggregate_statements
from ftm_lakehouse.service.journal import JournalWriter
from ftm_lakehouse.service.statements import StatementStore

csv.field_size_limit(sys.maxsize)


class DatasetEntities(LakeMixin):
    """
    Unified interface for entity operations in a dataset.

    This is the primary interface for client applications to work with entities.
    It provides:

    - Reading entities from the exported JSON file (iterate/stream)
    - Writing entities to the journal (add/bulk)
    - Querying entities from the statement store (get/query)
    - Export operations (export to JSON, statistics)

    Example:
        ```python
        from ftm_lakehouse import io

        dataset = io.ensure_dataset("my_dataset")

        # Write entities
        with dataset.entities.bulk(origin="source") as bulk:
            bulk.add_entity(entity)

        # Read a specific entity
        entity = dataset.entities.get("entity-id")

        # Stream all entities
        for entity in dataset.entities.iterate():
            process(entity)
        ```
    """

    @cached_property
    def _store(self) -> StatementStore:
        """Internal statement store."""
        return StatementStore(name=self.name, uri=self.uri)

    # -------------------------------------------------------------------------
    # Write operations
    # -------------------------------------------------------------------------

    def add(self, entity: EntityProxy, origin: str | None = None) -> None:
        """
        Add an entity to the journal for later flushing to the lake.

        Args:
            entity: The entity to add
            origin: Optional origin/source identifier
        """
        with self.tags.touch(tag.JOURNAL_UPDATED):
            self._store._journal.add_entity(entity, origin)

    def add_statement(self, stmt: Statement, origin: str | None = None) -> None:
        """
        Add a statement to the journal for later flushing to the lake.

        Args:
            stmt: The statement to add
            origin: Optional origin/source identifier (overrides stmt.origin)
        """
        with self.tags.touch(tag.JOURNAL_UPDATED):
            if origin:
                stmt.origin = origin
            with self._store._journal.writer(origin=origin) as w:
                w.add_statement(stmt)

    @contextlib.contextmanager
    def bulk(self, origin: str | None = None) -> Generator[JournalWriter, None, None]:
        """
        Get a bulk writer for adding entities to the journal.

        Args:
            origin: Optional origin/source identifier for all entities

        Yields:
            JournalWriter context manager

        Example:
            ```python
            with dataset.entities.bulk(origin="import") as bulk:
                for entity in entities:
                    bulk.add_entity(entity)
            ```
        """
        with self._store.bulk(origin) as writer:
            yield writer

    def flush(self) -> int:
        """
        Flush the journal to the lake storage.

        Returns:
            Number of statements flushed
        """
        return self._store.flush_journal()

    # -------------------------------------------------------------------------
    # Read operations
    # -------------------------------------------------------------------------

    def get(
        self,
        entity_id: str,
        origin: str | None = None,
        bucket: str | None = None,
    ) -> StatementEntity | None:
        """
        Retrieve an entity by ID from the statement store.

        Args:
            entity_id: The entity ID to look up
            origin: Optional filter by origin
            bucket: Optional filter by bucket

        Returns:
            The entity or None if not found
        """
        return self._store.get(entity_id, origin, bucket)

    def query(
        self,
        entity_ids: Iterable[str] | None = None,
        origin: str | None = None,
        bucket: str | None = None,
    ) -> StatementEntities:
        """
        Query entities from the statement store.

        Args:
            entity_ids: Optional list of entity IDs to filter
            origin: Optional filter by origin
            bucket: Optional filter by bucket

        Yields:
            Matching entities
        """
        yield from self._store.iterate(entity_ids, origin, bucket)

    def iterate(self) -> ValueEntities:
        """
        Iterate through all entities from the exported JSON file.

        This reads from the pre-exported `entities.ftm.json` file, which is
        faster than querying the statement store for full dataset iteration.

        Yields:
            All entities in the dataset
        """
        try:
            with self.storage.open(path.ENTITIES_JSON) as h:
                yield from smart_read_proxies(h)
        except DoesNotExist:
            return

    # -------------------------------------------------------------------------
    # Export operations
    # -------------------------------------------------------------------------

    @skip_if_latest(path.ENTITIES_JSON, [path.EXPORTS_STATEMENTS])
    def export(self) -> None:
        """
        Export entities from statements CSV to entities JSON.

        Reads the sorted statements.csv and aggregates them into entities,
        writing to entities.ftm.json.
        """
        with self.storage.open(path.EXPORTS_STATEMENTS) as i:
            statements = read_csv_statements(cast(BinaryIO, i))
            entities = aggregate_statements(statements, self.name)
            with self.storage.open(path.ENTITIES_JSON, "wb") as o:
                smart_write_proxies(cast(BinaryIO, o), entities)

    def export_statements(self) -> None:
        """Export the statement store to sorted statements.csv."""
        self._store.export_statements()

    def export_statistics(self) -> None:
        """Compute and export dataset statistics."""
        self._store.export_statistics()

    def get_statistics(self) -> DatasetStats:
        """Get dataset statistics, computing if necessary."""
        key = path.STATISTICS
        if self.storage.exists(key):
            return self.storage.get(key, model=DatasetStats)
        self.export_statistics()
        return self.storage.get(key, model=DatasetStats)

    # -------------------------------------------------------------------------
    # Maintenance operations
    # -------------------------------------------------------------------------

    def optimize(
        self, vacuum: bool | None = False, vacuum_keep_hours: int | None = 0
    ) -> None:
        """
        Optimize the statement store.

        Args:
            vacuum: Delete stale files after optimization
            vacuum_keep_hours: Keep files from last N hours when vacuuming
        """
        self._store.optimize(vacuum, vacuum_keep_hours)

    def get_changed_statements(
        self, start: int | None = None, end: int | None = None
    ) -> Generator[tuple[datetime, str, Statement], None, None]:
        """
        Get added/changed statements for a version range.

        Args:
            start: Starting version (default: 1)
            end: Ending version (default: latest)

        Yields:
            Tuples of (timestamp, change_type, statement)
        """
        yield from self._store.get_changed_statements(start, end)
