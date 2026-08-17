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
from anystore.store import Store
from anystore.util import join_uri, mask_uri
from followthemoney.dataset import DataResource
from ftmq.model.stats import DatasetStats
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM, Settings
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.factories import get_entities
from ftm_lakehouse.repository.job import JobRun

settings = Settings()


def _apply_resource(
    dataset: DatasetModel,
    store: Store,
    key: str,
    public_prefix: str,
    mime_type: str | None = None,
) -> None:
    if store.exists(key):
        info = store.info(key)
        public_url = join_uri(public_prefix, key)
        resource = DataResource(
            name=info.name,
            url=public_url,
            checksum=store.checksum(key, CHECKSUM_ALGORITHM),
            timestamp=info.created_at,
            mime_type=mime_type or info.mimetype,
            size=info.size,
        )
        dataset.resources.append(resource)


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


def _export_statements(op: "ExportOperation", *args, **kwargs) -> None:
    op.entities.export_statements_csv()


def _export_entities(
    op: "ExportOperation", run: JobRun[ExportJob], **kwargs: Any
) -> None:
    # export_entities prefers a fresh statements.csv on its own
    op.entities.export_entities()
    if run.job.make_diff:
        op.entities.export_diff()


def _export_documents(
    op: "ExportOperation", run: JobRun[ExportJob], **kwargs: Any
) -> None:
    op.documents.export_csv()
    if run.job.make_diff:
        op.documents.export_diff()


def _export_statistics(op: "ExportOperation", *args, **kwargs) -> None:
    stats = op.entities.stats()
    op._versions.make(path.EXPORTS_STATISTICS, stats)


def _export_index(op: "ExportOperation", *args, **kwargs) -> None:
    dataset = op._model
    store = get_store(dataset.uri)
    public_prefix = dataset.get_public_prefix()

    if public_prefix:
        entities = get_entities(dataset.name, dataset.uri)
        for key, mime_type in (
            (entities.EXPORTS_STATEMENTS, CSV),
            (entities.ENTITIES_JSON, FTM),
            (path.EXPORTS_DOCUMENTS, CSV),
            (path.EXPORTS_STATISTICS, JSON),
        ):
            _apply_resource(dataset, store, key, public_prefix, mime_type)

    if store.exists(path.EXPORTS_STATISTICS):
        dataset.apply_stats(store.get(path.EXPORTS_STATISTICS, model=DatasetStats))

    op._versions.make(path.INDEX, dataset)


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
        if not self._tags.is_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED]):
            self.entities.flush()
        if not self.entities.exists:
            self.log.info(
                "Statement store empty, skipping ...",
                uri=mask_uri(self.entities.uri),
            )
            return False
        return True

    def handle(self, run: JobRun[ExportJob], *args: Any, **kwargs: Any) -> None:
        has_statements = self.ensure_flush()
        if self.spec.requires_statements and not has_statements:
            return
        self.spec.handler(self, run, **kwargs)
        run.job.done = 1
