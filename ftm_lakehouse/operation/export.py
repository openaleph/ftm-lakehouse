"""Export operations (parquet -> statements.csv, entities.ftm.json, statistics.json)"""

from typing import TypeVar

from anystore import get_store
from anystore.types import HttpUrlStr
from anystore.util import join_uri
from ftmq.io import smart_write_proxies
from ftmq.model.dataset import make_dataset
from ftmq.model.stats import DatasetStats

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
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
from ftm_lakehouse.util import render

settings = Settings()


class BaseExportJob(DatasetJobModel):
    target: str
    # Include JOURNAL_UPDATED so we don't skip when there's unflushed data
    dependencies: list[str] = [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED]


J = TypeVar("J", bound=BaseExportJob)


class ExportStatementsJob(BaseExportJob):
    target: str = path.EXPORTS_STATEMENTS


class ExportEntitiesJob(BaseExportJob):
    target: str = path.ENTITIES_JSON
    make_diff: bool = True


class ExportStatisticsJob(BaseExportJob):
    target: str = path.EXPORTS_STATISTICS


class ExportDocumentsJob(BaseExportJob):
    target: str = path.EXPORTS_DOCUMENTS
    make_diff: bool = True
    public_url_prefix: HttpUrlStr | None = None

    def get_public_prefix(self) -> str | None:
        if self.public_url_prefix:
            return self.public_url_prefix
        if settings.public_url_prefix:
            return render(settings.public_url_prefix, {"dataset": self.dataset})


class ExportIndexJob(BaseExportJob):
    target: str = path.INDEX
    dependencies: list[str] = [
        path.CONFIG,
        path.EXPORTS_STATISTICS,
        path.ENTITIES_JSON,
        path.EXPORTS_DOCUMENTS,
    ]


class BaseExportOperation(DatasetJobOperation[J]):
    def get_target(self) -> str:
        return self.job.target

    def get_dependencies(self) -> list[str]:
        return self.job.dependencies

    def ensure_flush(self) -> bool:
        if not self.tags.is_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED]):
            self.entities.flush()
        if not self.entities._statements.exists:
            self.log.info(
                "Statement store empty, skipping ...", uri=self.entities._statements.uri
            )
            return False
        return True


class ExportStatementsOperation(BaseExportOperation[ExportStatementsJob]):
    """Export parquet store to statements.csv. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        if self.ensure_flush():
            output_uri = self.entities._store.to_uri(path.EXPORTS_STATEMENTS)
            self.entities._store.ensure_parent(path.EXPORTS_STATEMENTS)
            self.entities._statements.export_csv(output_uri)
            run.job.done = 1


class ExportEntitiesOperation(BaseExportOperation[ExportEntitiesJob]):
    """Export parquet store to entities.ftm.json. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun[ExportEntitiesJob], *args, **kwargs) -> None:
        if self.ensure_flush():
            output_uri = self.entities._store.to_uri(path.ENTITIES_JSON)
            smart_write_proxies(output_uri, self.entities.query(flush_first=False))
            if run.job.make_diff:
                self.entities.export_diff()
            run.job.done = 1


class ExportStatisticsOperation(BaseExportOperation[ExportStatisticsJob]):
    """Export parquet store statistics to statistics.json. Checks if journal
    needs to be flushed first. Skips if the last export is newer then last
    statements update."""

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        if self.ensure_flush():
            stats = self.entities.make_statistics()
            self.versions.make(path.EXPORTS_STATISTICS, stats)
            run.job.done = 1


class ExportDocumentsOperation(BaseExportOperation[ExportDocumentsJob]):
    """Export file metadata to documents.csv. Checks if journal needs to be
    flushed first. Skips if the last export is newer then last statements
    update."""

    def handle(self, run: JobRun[ExportDocumentsJob], *args, **kwargs) -> None:
        if self.ensure_flush():
            public_prefix = run.job.get_public_prefix()
            self.documents.export_csv(public_prefix)
            if run.job.make_diff:
                self.documents.export_diff(public_url_prefix=public_prefix)
            run.job.done = 1


class ExportIndexOperation(BaseExportOperation[ExportIndexJob]):
    """Export index.json, optionally including resources, therefore these
    targets need to be existing."""

    def handle(
        self,
        run: JobRun[ExportIndexJob],
        dataset: DatasetModel | None = None,
        *args,
        **kwargs,
    ) -> None:
        self.ensure_flush()

        if dataset is None:
            # we need a stub dataset to patch
            dataset = make_dataset(run.job.dataset, DatasetModel, uri=self.versions.uri)
        public_prefix = dataset.get_public_prefix()

        if public_prefix:
            store = get_store(dataset.uri)
            if store.exists(path.EXPORTS_STATEMENTS):
                uri = join_uri(dataset.uri, path.EXPORTS_STATEMENTS)
                public_url = join_uri(public_prefix, path.EXPORTS_STATEMENTS)
                dataset.resources.append(make_statements_resource(uri, public_url))

            if store.exists(path.ENTITIES_JSON):
                uri = join_uri(dataset.uri, path.ENTITIES_JSON)
                public_url = join_uri(public_prefix, path.ENTITIES_JSON)
                dataset.resources.append(make_entities_resource(uri, public_url))

            if store.exists(path.EXPORTS_DOCUMENTS):
                uri = join_uri(dataset.uri, path.EXPORTS_DOCUMENTS)
                public_url = join_uri(public_prefix, path.EXPORTS_DOCUMENTS)
                dataset.resources.append(make_documents_resource(uri, public_url))

            if store.exists(path.EXPORTS_STATISTICS):
                uri = join_uri(dataset.uri, path.EXPORTS_STATISTICS)
                public_url = join_uri(public_prefix, path.EXPORTS_STATISTICS)
                dataset.resources.append(make_statistics_resource(uri, public_url))
                dataset.apply_stats(
                    store.get(path.EXPORTS_STATISTICS, model=DatasetStats)
                )

        self.versions.make(path.INDEX, dataset)

        run.job.done = 1
