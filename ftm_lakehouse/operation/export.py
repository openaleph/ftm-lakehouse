"""Export operations (parquet -> statements.csv, entities.ftm.json,
statistics.json, documents.csv, index.json).

All exports run through a single :class:`ExportOperation` parameterized by
:class:`ExportKind`. Per-kind behavior lives in the :data:`EXPORTS` spec
table – adding a new export means adding a handler function and a spec
entry, not a new job / operation / factory triple.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable

from anystore import get_store
from anystore.types import HttpUrlStr
from anystore.util import join_uri, mask_uri
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


class ExportKind(StrEnum):
    """The available dataset exports."""

    statements = "statements"
    entities = "entities"
    documents = "documents"
    statistics = "statistics"
    index = "index"  # type: ignore[assignment]  # shadows str.index, fine for enums


class ExportJob(DatasetJobModel):
    """Job model for all export kinds."""

    kind: ExportKind
    make_diff: bool = True
    """Also export a delta diff file (``entities`` / ``documents`` kinds)."""
    public_url_prefix: HttpUrlStr | None = None
    """Override the public url prefix (``documents`` kind)."""

    def get_public_prefix(self) -> str | None:
        if self.public_url_prefix:
            return self.public_url_prefix
        if settings.public_url_prefix:
            return render(settings.public_url_prefix, {"dataset": self.dataset})


def _export_statements(op: "ExportOperation", run: JobRun, **kwargs: Any) -> None:
    op.entities._store.ensure_parent(path.EXPORTS_STATEMENTS)
    op.entities._statements.export_csv(path.EXPORTS_STATEMENTS)


def _export_entities(op: "ExportOperation", run: JobRun, **kwargs: Any) -> None:
    csv_uri = op._get_fresh_statements_csv()
    op.entities.export_entities(statements_csv_uri=csv_uri)
    if run.job.make_diff:
        op.entities.export_diff()


def _export_documents(op: "ExportOperation", run: JobRun, **kwargs: Any) -> None:
    public_prefix = run.job.get_public_prefix()
    op.documents.export_csv(public_prefix)
    if run.job.make_diff:
        op.documents.export_diff(public_url_prefix=public_prefix)


def _export_statistics(op: "ExportOperation", run: JobRun, **kwargs: Any) -> None:
    stats = op.entities.get_statistics()
    op.versions.make(path.EXPORTS_STATISTICS, stats)


def _export_index(
    op: "ExportOperation",
    run: JobRun,
    dataset: DatasetModel | None = None,
    **kwargs: Any,
) -> None:
    if dataset is None:
        # Prefer the bound Dataset's model (set by ``from_job``); fall back
        # to a stub dataset to patch when the operation was constructed
        # directly from a uri.
        bound = getattr(op, "_dataset", None)
        if bound is not None:
            dataset = bound.model
        else:
            dataset = make_dataset(run.job.dataset, DatasetModel, uri=op.versions.uri)

    store = get_store(dataset.uri)
    public_prefix = dataset.get_public_prefix()

    if public_prefix:
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

    if store.exists(path.EXPORTS_STATISTICS):
        dataset.apply_stats(store.get(path.EXPORTS_STATISTICS, model=DatasetStats))

    op.versions.make(path.INDEX, dataset)


@dataclass(frozen=True)
class ExportSpec:
    """Per-kind export behavior: freshness target, dependencies, handler."""

    target: str
    handler: Callable[..., None]
    # Include JOURNAL_UPDATED so we don't skip when there's unflushed data
    dependencies: tuple[str, ...] = (tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED)
    requires_statements: bool = True
    """Skip the handler when the statement store is empty."""


EXPORTS: dict[ExportKind, ExportSpec] = {
    ExportKind.statements: ExportSpec(path.EXPORTS_STATEMENTS, _export_statements),
    ExportKind.entities: ExportSpec(path.ENTITIES_JSON, _export_entities),
    ExportKind.documents: ExportSpec(path.EXPORTS_DOCUMENTS, _export_documents),
    ExportKind.statistics: ExportSpec(path.EXPORTS_STATISTICS, _export_statistics),
    ExportKind.index: ExportSpec(
        path.INDEX,
        _export_index,
        dependencies=(
            path.CONFIG,
            path.EXPORTS_STATISTICS,
            path.ENTITIES_JSON,
            path.EXPORTS_DOCUMENTS,
        ),
        requires_statements=False,
    ),
}


class ExportOperation(DatasetJobOperation[ExportJob]):
    """Export the dataset, dispatched by ``job.kind`` via :data:`EXPORTS`.

    Checks if the journal needs to be flushed first. Skips if the last
    export is newer than the last statements update (per-kind freshness
    target / dependencies from the spec table).
    """

    @property
    def spec(self) -> ExportSpec:
        return EXPORTS[self.job.kind]

    def get_target(self) -> str:
        return self.spec.target

    def get_dependencies(self) -> list[str]:
        return list(self.spec.dependencies)

    def ensure_flush(self) -> bool:
        if not self.tags.is_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED]):
            self.entities.flush()
        if not self.entities._statements.exists:
            self.log.info(
                "Statement store empty, skipping ...",
                uri=mask_uri(self.entities._statements.uri),
            )
            return False
        return True

    def _get_fresh_statements_csv(self) -> str | None:
        """Return statements.csv URI if it's at least as fresh as the store.

        The statements export's freshness tag is its target key
        (``path.EXPORTS_STATEMENTS``), touched after a successful run.
        """
        store = self.entities._store
        if not store.exists(path.EXPORTS_STATEMENTS):
            return None
        if self.tags.is_latest(path.EXPORTS_STATEMENTS, [tag.STATEMENTS_UPDATED]):
            return store.to_uri(path.EXPORTS_STATEMENTS)
        return None

    def handle(self, run: JobRun, *args: Any, **kwargs: Any) -> None:
        has_statements = self.ensure_flush()
        if self.spec.requires_statements and not has_statements:
            return
        self.spec.handler(self, run, **kwargs)
        run.job.done = 1
