"""Export operations (parquet -> statements.csv, entities.ftm.json,
documents.csv, statistics.json, index.json) plus their diff series.

Every artifact that is a function of the entity stream is written in **one
sweep**: `ExportOperation.export` scans the statement store once
([`ParquetStore.sweep`][ftm_lakehouse.storage.parquet.ParquetStore.sweep]),
folds the rows into entities and hands each one to every artifact the run
covers.

What an artifact *is* – where it lives, whether it is current, how it is
written, what its diff series does with an entity – belongs to
[`ArtifactsRepository`][ftm_lakehouse.repository.artifacts.ArtifactsRepository].
This operation decides only which artifacts a run covers, and drives the
stream through them.

`ExportKind` selects them; [`ExportKind.all`][ftm_lakehouse.repository.artifacts.ExportKind]
covers every streamed one. The two artifacts that are not functions of the
entity stream – ``statistics.json`` (a global SQL aggregate) and ``index.json``
(store metadata) – are written directly, outside the sweep.
"""

from datetime import datetime
from functools import cached_property
from typing import Any, Iterator

from anystore.util import mask_uri
from ftmq.model.stats import DatasetStats
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities.aggregate import EntityPayload, aggregate_unsafe
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.artifacts import SWEEP_KINDS, ExportKind
from ftm_lakehouse.repository.job import JobRun

__all__ = ["ExportJob", "ExportKind", "ExportOperation", "MAKE_KINDS", "SWEEP_KINDS"]

settings = Settings()

MAKE_KINDS = (ExportKind.all, ExportKind.statistics, ExportKind.index)
"""What a full ``make`` runs, in order. ``all`` covers `SWEEP_KINDS` in one
pass; ``index`` goes last because it registers what the others wrote."""


class ExportJob(DatasetJobModel):
    """Job model for all export kinds."""

    kind: ExportKind = ExportKind.all
    make_diff: bool = True
    """Also export delta diff files (``entities`` / ``documents`` kinds)."""
    result: dict[str, int] | None = None
    """What the run wrote, per artifact and per diff op."""


class ExportOperation(DatasetJobOperation[ExportJob]):
    """Export the dataset, in one sweep over the entity stream.

    Flushes and merges first ([`prepare`][ExportOperation.prepare]) – exports
    read canonical rows. Skips if the target is newer than the last optimize.

    A run stamps a freshness tag per artifact it wrote, so a later single-kind
    export sees itself up to date and ``index.json`` still finds the
    dependencies it registers.
    """

    @cached_property
    def kinds(self) -> tuple[ExportKind, ...]:
        """The sweep artifacts this run writes."""
        if self.job.kind == ExportKind.all:
            return SWEEP_KINDS
        if self.job.kind in SWEEP_KINDS:
            return (self.job.kind,)
        return ()

    def get_target(self) -> str:
        if self.job.kind == ExportKind.all:
            return tag.OP_EXPORT
        return str(self.artifacts[self.job.kind].tag)

    def get_dependencies(self) -> list[str]:
        if self.job.kind == ExportKind.all:
            return [tag.STATEMENTS_OPTIMIZED]
        return [str(d) for d in self.artifacts[self.job.kind].dependencies]

    def prepare(self) -> None:
        """Drain the journal and merge, so the export reads canonical rows."""
        if not self._tags.is_latest(tag.JOURNAL_FLUSHED, [tag.JOURNAL_UPDATED]):
            self.entities.flush()
        if self.entities.exists and self.entities.needs_merge:
            self.entities.merge()

    def iterate(self) -> Iterator[EntityPayload]:
        """Every entity in the store, folded from one scan.

        Writes ``statements.csv`` from the same Arrow batches when this run
        covers it, so the csv costs a tee rather than a second pass. Rows are
        only materialised when something downstream needs them – a
        statements-only export stays columnar end to end.
        """
        with_csv_export = ExportKind.statements in self.kinds
        tee = bool({ExportKind.entities, ExportKind.documents} & set(self.kinds))
        rows = self.entities.sweep(with_csv_export=with_csv_export, tee=tee)
        yield from aggregate_unsafe(rows, self.dataset)

    def export(self, now: datetime) -> dict[str, int]:
        """Write every requested artifact from one pass over the entities.

        Args:
            now: Timestamp the run started – the diff files are named after it
                and the diff states are recorded at it.

        Returns:
            Counts per artifact and per diff op.
        """
        version = self.entities.version
        if self.job.make_diff and version is not None and self.entities.needs_merge:
            raise RuntimeError(
                "Cannot export diffs: the statement store has un-merged writes "
                "and a diff publishes canonical entities. Run "
                "`ftm-lakehouse maintenance optimize` first."
            )
        session = self.artifacts.session(now, self.kinds, version, self.job.make_diff)
        with session:
            for payload in self.iterate():
                session.consume(payload)
        return session.result()

    def export_statistics(self) -> None:
        """Write ``statistics.json`` from the store's global SQL aggregate."""
        self.artifacts.statistics.write(self.entities.stats())

    def export_index(self) -> None:
        """Write ``index.json``, registering what the exports produced."""
        dataset = self._model
        dataset.resources = list(self.artifacts.resources())
        statistics = self.artifacts.statistics
        if statistics.exists():
            dataset.apply_stats(self._store.get(statistics.key, model=DatasetStats))
        self.artifacts.index.write(dataset)

    def handle(self, run: JobRun[ExportJob], *args: Any, **kwargs: Any) -> None:
        if run.job.kind == ExportKind.index:
            self.export_index()
            run.job.done = 1
            return

        if not self.entities.exists:
            self.log.info(
                "Statement store empty, skipping ...",
                uri=mask_uri(self.entities.uri),
            )
            return

        if run.job.kind == ExportKind.statistics:
            self.export_statistics()
            run.job.done = 1
            return

        started = utc_now()
        result = self.export(started)
        for artifact in self.artifacts.written_by(self.kinds):
            artifact.touch(started)
        self.log.info("Export(s) done.", **result)
        run.job.result = result
        run.job.done = 1
