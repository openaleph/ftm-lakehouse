"""Export operations (parquet -> statements.csv, entities.ftm.json, statistics.json)"""

from typing import TypeVar

from anystore.util import join_uri
from ftmq.io import smart_write_proxies
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.helpers.dataset import (
    make_documents_resource,
    make_entities_resource,
    make_statements_resource,
    make_statistics_resource,
)
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class BaseExportJob(DatasetJobModel):
    target: str
    # Include JOURNAL_UPDATED so we don't skip when there's unflushed data
    dependencies: list[str] = [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]


J = TypeVar("J", bound=BaseExportJob)


class ExportStatementsJob(BaseExportJob):
    target: str = path.EXPORTS_STATEMENTS


class ExportEntitiesJob(BaseExportJob):
    target: str = path.ENTITIES_JSON


class ExportStatisticsJob(BaseExportJob):
    target: str = path.STATISTICS


class ExportDocumentsJob(BaseExportJob):
    target: str = path.EXPORTS_DOCUMENTS


class ExportIndexJob(BaseExportJob):
    target: str = path.INDEX
    dependencies: list[str] = [
        path.CONFIG,
        path.STATISTICS,
        path.ENTITIES_JSON,
        path.EXPORTS_DOCUMENTS,
    ]
    include_statements_csv: bool = False
    include_entities_json: bool = False
    include_documents_csv: bool = False
    include_statistics: bool = False


class BaseExportOperation(DatasetJobOperation[J]):
    def get_target(self) -> str:
        return self.job.target

    def get_dependencies(self) -> list[str]:
        return self.job.dependencies

    def ensure_flush(self) -> None:
        if not self.tags.is_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED]):
            self.entities.flush()

    def export_statements(self) -> None:
        self.ensure_flush()
        output_uri = self.entities._store.get_key(path.EXPORTS_STATEMENTS)
        self.entities._store.ensure_parent(path.EXPORTS_STATEMENTS)
        self.entities._statements.export_csv(output_uri)

    def export_entities(self) -> None:
        self.ensure_flush()
        output_uri = self.entities._store.get_key(path.ENTITIES_JSON)
        smart_write_proxies(output_uri, self.entities.query())

    def export_documents(self, public_prefix: str | None = None) -> None:
        self.ensure_flush()
        self.documents.export_csv(public_prefix)

    def export_statistics(self) -> None:
        self.ensure_flush()
        stats = self.entities.make_statistics()
        self.versions.make(path.STATISTICS, stats)


class ExportStatementsOperation(BaseExportOperation[ExportStatementsJob]):
    """Export parquet store to statements.csv. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        self.export_statements()
        run.job.done = 1


class ExportEntitiesOperation(BaseExportOperation[ExportEntitiesJob]):
    """Export parquet store to entities.ftm.json. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        self.export_entities()
        run.job.done = 1


class ExportStatisticsOperation(BaseExportOperation[ExportStatisticsJob]):
    """Export parquet store statistics to statistics.json. Checks if journal
    needs to be flushed first. Skips if the last export is newer then last
    statements update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        self.export_statistics()
        run.job.done = 1


class ExportDocumentsOperation(BaseExportOperation[ExportDocumentsJob]):
    """Export file metadata to documents.csv. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        self.export_documents()
        run.job.done = 1


class ExportIndexOperation(BaseExportOperation[ExportIndexJob]):
    """Export index.json, optionally including statistics and url to entities.ftm.json,
    therefore these targets need to be done as well."""

    def handle(
        self,
        run: JobRun[ExportIndexJob],
        dataset: DatasetModel,
        *args,
        **kwargs,
    ) -> None:
        self.ensure_flush()
        force = kwargs.get("force", False)
        public_prefix = dataset.get_public_prefix()

        if run.job.include_statements_csv:
            if force or not self.tags.is_latest(
                path.EXPORTS_STATEMENTS, [tag.STATEMENTS_UPDATED]
            ):
                with self.tags.touch(path.EXPORTS_STATEMENTS):
                    self.export_statements()
            if public_prefix:
                uri = join_uri(dataset.uri, path.EXPORTS_STATEMENTS)
                public_url = join_uri(public_prefix, path.EXPORTS_STATEMENTS)
                dataset.resources.append(make_statements_resource(uri, public_url))

        if run.job.include_entities_json:
            if force or not self.tags.is_latest(
                path.ENTITIES_JSON, [tag.STATEMENTS_UPDATED]
            ):
                with self.tags.touch(path.ENTITIES_JSON):
                    self.export_entities()
            if public_prefix:
                uri = join_uri(dataset.uri, path.ENTITIES_JSON)
                public_url = join_uri(public_prefix, path.ENTITIES_JSON)
                dataset.resources.append(make_entities_resource(uri, public_url))

        if run.job.include_documents_csv:
            if force or not self.tags.is_latest(
                path.EXPORTS_DOCUMENTS, [tag.STATEMENTS_UPDATED]
            ):
                with self.tags.touch(path.EXPORTS_DOCUMENTS):
                    self.export_documents(public_prefix)
            if public_prefix:
                uri = join_uri(dataset.uri, path.EXPORTS_DOCUMENTS)
                public_url = join_uri(public_prefix, path.EXPORTS_DOCUMENTS)
                dataset.resources.append(make_documents_resource(uri, public_url))

        if run.job.include_statistics:
            if force or not self.tags.is_latest(
                path.STATISTICS, [tag.STATEMENTS_UPDATED]
            ):
                with self.tags.touch(path.STATISTICS):
                    self.export_statistics()
            if public_prefix:
                uri = join_uri(dataset.uri, path.STATISTICS)
                public_url = join_uri(public_prefix, path.STATISTICS)
                dataset.resources.append(make_statistics_resource(uri, public_url))

        # update dataset with computed stats
        stats = self.versions.get(path.STATISTICS, model=DatasetStats)
        if stats:
            dataset.apply_stats(stats)

        self.versions.make(path.INDEX, dataset)

        run.job.done = 1
