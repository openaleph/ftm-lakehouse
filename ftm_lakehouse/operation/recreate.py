"""Recreate operation - repair corrupted datasets from exported files."""

from enum import Enum

from anystore.io import smart_open
from followthemoney.statement.serialize import read_csv_statements

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class RecreateSource(str, Enum):
    """Source for recreating the statement store."""

    AUTO = "auto"  # Automatically choose based on tag timestamps
    ENTITIES = path.ENTITIES_JSON
    STATEMENTS = path.EXPORTS_STATEMENTS


class RecreateJob(DatasetJobModel):
    """Job model for recreate operation."""

    source: RecreateSource = RecreateSource.AUTO
    statements_imported: int = 0
    entities_imported: int = 0
    files_imported: int = 0


class RecreateOperation(DatasetJobOperation[RecreateJob]):
    """
    Recreate a corrupted dataset by rebuilding the parquet store from exports.

    This operation repairs corrupted lakehouse datasets by:
    1. Clearing the statement store (parquet) and journal
    2. Re-importing entities/statements from the most recent export

    The source for re-import is selected based on tag timestamps:
    - If entities.ftm.json is newer, import entities
    - If statements.csv is newer, import statements
    - Can be forced to use a specific source via the job's `source` field

    Warning: This operation is destructive - it will delete all existing
    statement data before re-importing from exports.
    """

    target = tag.OP_RECREATE
    dependencies = []  # No automatic freshness check - always run when called

    def _get_source(self) -> RecreateSource:
        """Determine which export to use based on tag timestamps or job config."""
        if self.job.source != RecreateSource.AUTO:
            return self.job.source

        entities_ts = self.tags.get(tag.ENTITIES_JSON)
        statements_ts = self.tags.get(tag.EXPORTS_STATEMENTS)

        entities_exists = self.entities._store.exists(path.ENTITIES_JSON)
        statements_exists = self.entities._store.exists(path.EXPORTS_STATEMENTS)

        if not entities_exists and not statements_exists:
            raise RuntimeError(
                "No export files found. Cannot recreate dataset without "
                f"`{path.ENTITIES_JSON}` or `{path.EXPORTS_STATEMENTS}`"
            )

        if not entities_exists:
            return RecreateSource.STATEMENTS
        if not statements_exists:
            return RecreateSource.ENTITIES

        # Both exist, compare timestamps
        if entities_ts is None and statements_ts is None:
            # No tags, prefer statements (faster)
            return RecreateSource.STATEMENTS
        if entities_ts is None:
            return RecreateSource.STATEMENTS
        if statements_ts is None:
            return RecreateSource.ENTITIES

        # Return the most recent one
        return (
            RecreateSource.ENTITIES
            if entities_ts >= statements_ts
            else RecreateSource.STATEMENTS
        )

    def _import_from_entities(self, run: JobRun[RecreateJob]) -> None:
        """Import entities from entities.ftm.json."""
        uri = self.entities._store.to_uri(path.ENTITIES_JSON)

        self.log.info(f"Importing from `{path.ENTITIES_JSON}` ...", uri=uri)

        with self.entities.bulk() as writer:
            for entity in self.entities.stream():
                writer.add_entity(entity)
                run.job.entities_imported += 1

                if run.job.entities_imported % 10_000 == 0:
                    self.log.info(
                        f"Importing from `{path.ENTITIES_JSON}` ...",
                        entities=run.job.entities_imported,
                        uri=uri,
                    )
                    run.save()
        self.log.info(
            f"Importing from `{path.ENTITIES_JSON}` done.",
            entities=run.job.entities_imported,
            uri=uri,
        )
        run.save()

    def _import_from_statements(self, run: JobRun[RecreateJob]) -> None:
        """Import statements from statements.csv."""
        uri = self.entities._store.to_uri(path.EXPORTS_STATEMENTS)

        self.log.info(f"Importing from `{path.EXPORTS_STATEMENTS}` ...", uri=uri)

        with self.entities.bulk() as writer:
            with smart_open(uri, "rb") as fh:
                for stmt in read_csv_statements(fh):  # type: ignore[arg-type]
                    writer.add_statement(stmt)
                    run.job.statements_imported += 1

                    if run.job.statements_imported % 100_000 == 0:
                        self.log.info(
                            f"Importing from `{path.EXPORTS_STATEMENTS}` ...",
                            statements=run.job.statements_imported,
                            uri=uri,
                        )
                        run.save()
        self.log.info(
            f"Importing from `{path.EXPORTS_STATEMENTS}` done.",
            statements=run.job.statements_imported,
            uri=uri,
        )
        run.save()

    def _import_from_archive(self, run: JobRun[RecreateJob]) -> None:
        """Collect files metadata to add document entities"""
        self.log.info("Importing from archive ...", uri=self.archive.uri)

        with self.entities.bulk(origin=tag.CRAWL_ORIGIN) as writer:
            for file in self.archive.iterate_files():
                if file.origin == tag.CRAWL_ORIGIN:
                    for entity in file.make_entities():
                        writer.add_entity(entity)
                run.job.files_imported += 1

                if run.job.files_imported % 1_000 == 0:
                    self.log.info(
                        "Importing from archive ...",
                        files=run.job.files_imported,
                        uri=self.archive.uri,
                    )
                    run.save()
        self.log.info(
            "Importing from archive done.",
            files=run.job.files_imported,
            uri=self.archive.uri,
        )
        run.save()

    def handle(self, run: JobRun[RecreateJob], *args, **kwargs) -> None:
        source = self._get_source()
        self.log.info("Recreating dataset", source=source.value)

        # Step 1: Clear the parquet statement store
        self.entities._statements.destroy()

        # Step 2: Re-import from export
        if source == RecreateSource.STATEMENTS:
            self._import_from_statements(run)
        else:
            self._import_from_entities(run)

        # Step 3: Collect file entities from "crawl" origin
        self._import_from_archive(run)

        # Step 4: Flush journal to parquet
        flushed = self.entities.flush()

        self.log.info(
            "Recreate complete",
            source=source.value,
            entities_imported=run.job.entities_imported,
            statements_imported=run.job.statements_imported,
            statements_flushed=flushed,
        )

        run.job.done = 1
