"""Export operations (parquet -> statements.csv, entities.ftm.json,
documents.csv, statistics.json, index.json) plus their diff series.

Every artifact that is a function of the entity stream is written in **one
sweep**: `ExportOperation.export` scans the statement store once
([`ParquetStore.sweep`][ftm_lakehouse.storage.parquet.ParquetStore.sweep]),
folds the rows into entities and hands each one to every artifact.

What an artifact *is* – where it lives, how it is written, what its diff
series does with an entity – belongs to
[`ArtifactsRepository`][ftm_lakehouse.repository.artifacts.ArtifactsRepository].
This operation only drives the stream through them.

A run writes every artifact
([`streamed`][ftm_lakehouse.repository.artifacts.ArtifactsRepository.streamed])
and then ``index.json``, which registers what they wrote. Everything but
``statements.csv`` is folded out of the entity stream – ``statistics.json``
among them
([`StatsCollector`][ftm_lakehouse.logic.entities.stats.StatsCollector]) – so
one scan of the store is what the whole set costs.
"""

from datetime import datetime
from typing import Any, Iterator

from anystore.io import SyncProgressBar
from anystore.io.progress import Throughput
from anystore.util import mask_uri
from ftmq.model.stats import DatasetStats
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.entities.aggregate import EntityPayload, aggregate_unsafe
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.artifacts import ExportKind
from ftm_lakehouse.repository.job import JobRun

__all__ = ["ExportJob", "ExportKind", "ExportOperation"]

settings = Settings()


class ExportJob(DatasetJobModel):
    """Job model for the export."""

    make_diff: bool = True
    """Also export the delta diff files of the diffable artifacts."""
    result: dict[str, int] | None = None
    """What the run wrote, per artifact and per diff op."""


class ExportOperation(DatasetJobOperation[ExportJob]):
    """Export the dataset, in one sweep over the entity stream.

    Flushes the journal first ([`prepare`][ExportOperation.prepare]) and reads
    the store as it is – reads reconcile un-merged rows, so no merge is
    needed. Skips if the target is newer than the last write.

    A run stamps a freshness tag per artifact it wrote, which is the record of
    when each one was last produced.
    """

    target = tag.OP_EXPORT
    dependencies = [tag.STATEMENTS_UPDATED]
    """The content clock – rows landing or an origin dropped. A merge rewrites
    files without changing content, so it leaves the exports fresh."""

    def prepare(self) -> None:
        """Drain the journal, so the export covers the rows still buffered.

        On an empty journal this is a ``LIMIT 1`` probe. Ahead of the
        freshness window, as the base class requires: a drain that lands rows
        moves [`STATEMENTS_UPDATED`][ftm_lakehouse.core.conventions.tag.STATEMENTS_UPDATED],
        which this operation depends on.
        """
        self.entities.flush()

    def iterate(self, throughput: Throughput | None = None) -> Iterator[EntityPayload]:
        """Every entity in the store, folded from one scan.

        ``statements.csv`` is written from the same Arrow batches the entities
        are folded out of, so the csv costs a tee rather than a second pass.

        Args:
            throughput: Counter fed the Arrow bytes the scan pulls – the
                progress bar's, so it shows how fast the sweep reads.
        """
        yield from aggregate_unsafe(self.entities.sweep(throughput), self.dataset)

    def export(self, now: datetime) -> dict[str, int]:
        """Write every streamed artifact from one pass over the entities.

        Args:
            now: Timestamp the run started – the diff files are named after it
                and the diff states are recorded at it.

        Held under the statement store's merge lock: the sweep pins one
        snapshot's files, and an ``optimize`` – a merge, then a retention-0
        vacuum – would delete them under it. Appends are not affected.

        Returns:
            Counts per artifact and per diff op.
        """
        with self.entities.merge_lock():
            version = self.entities.version
            session = self.artifacts.session(now, version, self.job.make_diff)
            count = self.entities._statements.num_rows
            # advanced per statement folded; its throughput is the Arrow
            # bytes the scan pulls from the store
            with session, SyncProgressBar("Exporting statements", count) as bar:
                for payload in self.iterate(bar.throughput):
                    session.consume(payload)
                    bar.advance(len(payload.statements))
            return session.result()

    def export_index(self) -> None:
        """Write ``index.json``, registering what the exports produced."""
        dataset = self._model
        dataset.resources = list(self.artifacts.resources())
        statistics = self.artifacts.statistics
        if statistics.exists():
            dataset.apply_stats(self._store.get(statistics.key, model=DatasetStats))
        self.artifacts.index.write(dataset)

    def handle(self, run: JobRun[ExportJob], *args: Any, **kwargs: Any) -> None:
        """One sweep, every artifact's freshness tag, then ``index.json``.

        The tags are stamped after the sweep returns, so a crash part-way
        stamps nothing. They are what each artifact was last written at, and
        `ftm_lakehouse.operation.download.DownloadArchiveOperation` keys its
        own freshness on one of them (``exports/documents.csv``).

        ``index.json`` runs last because it registers what the others wrote –
        and runs even on an empty store, where there is no sweep to do but the
        dataset still has metadata to publish.
        """
        if self.entities.exists:
            started = utc_now()
            result = self.export(started)
            for artifact in self.artifacts.streamed():
                artifact.touch(started)
            self.log.info("Export(s) done.", **result)
            run.job.result = result
        else:
            self.log.info(
                "Statement store empty, nothing to sweep ...",
                uri=mask_uri(self.entities.uri),
            )
        self.export_index()
        run.job.done = 1
