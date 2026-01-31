"""EntityRepository - entity/statement operations using JournalStore + ParquetStore."""

from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Iterable

import orjson
from anystore.io import smart_write_json
from anystore.store import get_store
from anystore.types import SDict, Uri
from anystore.util import Took
from followthemoney import EntityProxy, Statement, StatementEntity
from ftmq.io import smart_read_proxies
from ftmq.model.stats import DatasetStats
from ftmq.query import Query
from ftmq.types import StatementEntities, ValueEntities

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.repository.diff import ParquetDiffMixin
from ftm_lakehouse.storage import JournalStore, ParquetStore
from ftm_lakehouse.storage.journal import JournalWriter

settings = Settings()


class EntityRepository(ParquetDiffMixin, BaseRepository):
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
        self._store = get_store(uri)

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
        if self._journal.count() == 0:
            self.log.debug("Journal is empty", journal=self._journal.uri)
            # set tags for the initial run
            if not self._tags.exists(tag.JOURNAL_FLUSHED):
                self._tags.set(tag.JOURNAL_FLUSHED)
            if not self._tags.exists(tag.STATEMENTS_UPDATED):
                self._tags.set(tag.STATEMENTS_UPDATED)
            return 0

        with self._tags.touch(tag.JOURNAL_FLUSHED), Took() as t:
            self.log.info("Flushing journal ...", journal=self._journal.uri)

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

            self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Flushed statements from journal to lake",
                count=total_count,
                took=t.took,
                journal=self._journal.uri,
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

        Additional filter kwargs are passed to ftmq Query.

        Args:
            entity_ids: Filter by entity IDs
            flush_first: Flush journal before querying (default True)

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

    def stream(self) -> ValueEntities:
        """
        Stream entities from the exported JSON file.

        This reads from the pre-exported entities.ftm.json file,
        not directly from the parquet store.
        """
        uri = self._store.to_uri(path.ENTITIES_JSON)
        yield from smart_read_proxies(uri)

    def make_statistics(self) -> DatasetStats:
        """Compute statistics from the parquet store."""
        return self._statements.stats()

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_ENTITIES

    def _filter_changes(
        self,
        changes: Generator[tuple[datetime, str, Statement], None, None],
    ) -> set[str]:
        """Track all entity IDs that have any statement change."""
        changed_entity_ids: set[str] = set()
        for _, change_type, stmt in changes:
            if change_type in ("insert", "update_postimage"):
                changed_entity_ids.add(stmt.entity_id)
        return changed_entity_ids

    def _write_diff(self, entity_ids: set[str], v: int, ts: datetime, **kwargs) -> str:
        """Write entities as line-based JSON with operation envelopes."""
        key = path.entities_diff(v, ts)
        with self._store.open(key, "wb") as o:
            smart_write_json(o, self._get_delta_entities(entity_ids))
        return self._store.to_uri(key)

    def _get_delta_entities(self, entity_ids: set[str]) -> Generator[SDict, None, None]:
        for entity in self.query(entity_ids=entity_ids, flush_first=False):
            yield self._make_envelope(entity.to_dict())

    def _make_envelope(self, data: SDict, op: str = "ADD") -> SDict:
        return {"op": op, "entity": data}

    def _write_initial_diff(self, version: int, ts: datetime, **kwargs) -> None:
        """Copy over exported entities.ftm.json to initial diff version"""
        with self._store.open(path.entities_diff(version, ts), "wb") as o:
            for data in self._store.stream(path.ENTITIES_JSON):
                line = orjson.dumps(
                    self._make_envelope(data), option=orjson.OPT_APPEND_NEWLINE
                )
                o.write(line)
