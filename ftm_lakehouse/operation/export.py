"""The export: every artifact that is a function of the entity stream –
``statements.csv``, ``entities.ftm.json``, ``documents.csv`` per origin scope,
``statistics.json`` and their diff series – from one sweep over the statement
store, then ``index.json``.

What an artifact is and how it is written belongs to
[`ArtifactsRepository`][ftm_lakehouse.repository.artifacts.ArtifactsRepository];
this operation drives the stream through them.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from anystore.io import SyncProgressBar, smart_open
from anystore.util import Took, mask_uri
from ftmq.model.stats import DatasetStats
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.entities.aggregate import aggregate_unsafe
from ftm_lakehouse.logic.entities.stats import StatsCollector
from ftm_lakehouse.logic.parquet import worker_duckdb_config
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.model.statement import statement_csv_header
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.artifacts import (
    DiffableArtifact,
    ExportKind,
    ExportSession,
    StatisticsRun,
)
from ftm_lakehouse.repository.factories import get_artifacts
from ftm_lakehouse.repository.job import JobRun
from ftm_lakehouse.storage.parquet import partition_cursor, sweep_partition
from ftm_lakehouse.util import process_map

__all__ = ["ExportJob", "ExportKind", "ExportOperation"]

settings = Settings()


@dataclass
class ExportTask:
    """One ``(shard, bucket)`` pair's sweep as handed to a worker – plain data,
    resolved against the parent's snapshot, so no worker replays the log."""

    dataset: str
    uri: str
    now: datetime
    version: int | None
    make_diff: bool
    parts: str
    shard: str
    bucket: str
    source: str
    clean: bool
    duckdb_config: dict[str, str]
    pending: dict[str, frozenset[str]]


@dataclass
class ExportPart:
    """What one worker produced, as the parent needs it back."""

    parts: str
    counts: dict[str, int]
    seen: dict[str, frozenset[str]]
    stats: StatsCollector
    took: timedelta


def export_partition(task: ExportTask) -> ExportPart:
    """Sweep one ``(shard, bucket)`` pair into one part of every artifact.

    Prepares and closes its session but never finishes or commits it – that is
    work over the whole store, and the parent's. Silent: a spawned worker has no
    logging setup, so ``took`` travels back in the `ExportPart`.
    """
    with Took() as t:
        artifacts = get_artifacts(task.dataset, task.uri)
        # no delete-candidate scan: the parent ran it and hands out `pending`
        runs = artifacts.runs(task.now, task.parts)
        session = ExportSession(runs, task.version, task.make_diff)
        statements = artifacts.statements
        session.prepare()
        for run in session.diffable:
            run.pending |= set(task.pending.get(run.name, ()))
        try:
            with smart_open(
                statements.part(task.parts),
                "wb",
                compression=statements.compression,
            ) as csv:
                with partition_cursor(
                    task.source, task.clean, task.duckdb_config
                ) as cur:
                    rows = sweep_partition(cur, csv)
                    for payload in aggregate_unsafe(rows, task.dataset):
                        session.consume(payload)
        finally:
            session.close()
        seen = {
            run.name: frozenset(task.pending.get(run.name, ())) - run.pending
            for run in session.diffable
        }
        stats = next(r.collector for r in session.runs if isinstance(r, StatisticsRun))
        counts = dict(session.result())
    return ExportPart(task.parts, counts, seen, stats, t.took)


class ExportJob(DatasetJobModel):
    """Job model for the export."""

    make_diff: bool = True
    """Also export the delta diff files of the diffable artifacts."""
    result: dict[str, int] | None = None
    """What the run wrote, per artifact and per diff op."""


class ExportOperation(DatasetJobOperation[ExportJob]):
    """Export the dataset in one sweep over the entity stream – skipped while
    the exports are newer than the last content change."""

    target = tag.OP_EXPORT
    dependencies = [tag.STATEMENTS_UPDATED]
    """The content clock – a merge does not move it."""

    def prepare(self) -> None:
        """Drain the journal ahead of the freshness check, so buffered rows
        count."""
        self.entities.flush()

    def export(self, now: datetime) -> dict[str, int]:
        """Write every streamed artifact from one pass over the entities.

        Each ``(shard, bucket)`` pair is swept into parts of every artifact, in
        ``LAKEHOUSE_WORKERS`` processes, and the parts are concatenated. Held
        under the merge lock, so an ``optimize`` cannot vacuum the snapshot's
        files.

        Args:
            now: When the run started – diff files are named after it.

        Returns:
            Counts per artifact and per diff op.
        """
        workers = max(settings.workers, 1)
        store = self.entities._statements
        counts: Counter[str] = Counter()
        with (
            self.entities.merge_lock(),
            TemporaryDirectory(prefix="ftm-lakehouse-export-") as tmp,
        ):
            version = store.version
            sources = store.sweep_sources()
            # the parent's own part: the documents csv and the DELs of `finish`
            session = ExportSession(
                self.artifacts.runs(now, f"{tmp}/parent"),
                version,
                self.job.make_diff,
                self.entities.deleted_candidates,
            )
            parts = [f"{tmp}/{shard}-{bucket}" for (shard, bucket), _, _ in sources]
            with (
                session,
                SyncProgressBar("Exporting statements", store.num_rows) as bar,
                process_map(workers, ordered=False) as run,
            ):
                pending = self._pending_by_shard(session)
                tasks = [
                    ExportTask(
                        dataset=self.dataset,
                        uri=str(self.uri),
                        now=now,
                        version=version,
                        make_diff=self.job.make_diff,
                        parts=part,
                        shard=shard,
                        bucket=bucket,
                        source=source,
                        clean=clean,
                        duckdb_config=worker_duckdb_config(workers),
                        pending=pending.get(shard, {}),
                    )
                    for part, ((shard, bucket), source, clean) in zip(parts, sources)
                ]
                by_parts = {task.parts: task for task in tasks}
                done: dict[str, ExportPart] = {}
                for part in run(export_partition, tasks):
                    task = by_parts[part.parts]
                    done[part.parts] = part
                    statements = part.counts.get("statements", 0)
                    counts.update(part.counts)
                    bar.advance(statements)
                    self.log.info(
                        f"Swept pair `{task.shard}/{task.bucket}`.",
                        took=part.took,
                        shard=task.shard,
                        bucket=task.bucket,
                        statements=statements,
                    )
                # in snapshot order, not as finished: the documents csv is
                # written from the staged parts in the order they are adopted
                for part in (done[p] for p in parts):
                    session.adopt(part.parts, part.seen, part.stats)
            # after the session closed: its own writers' codec trailers are
            # written, so every part is a complete frame
            header = self._write_header(tmp, counts.get("statements", 0))
            counts.update(session.result())
            for artifact in self.artifacts.streamed():
                artifact.assemble([*header, *parts, f"{tmp}/parent"])
            for artifact in self.artifacts.streamed():
                if isinstance(artifact, DiffableArtifact):
                    artifact.assemble_diff(now, [*parts, f"{tmp}/parent"])
        return dict(counts)

    def _write_header(self, tmp: str, statements: int) -> list[str]:
        """The ``statements.csv`` header as the first part – none for an empty
        sweep, so an empty export's csv stays empty."""
        if not statements:
            return []
        artifact = self.artifacts.statements
        part = f"{tmp}/header"
        Path(part).mkdir(parents=True, exist_ok=True)
        with smart_open(
            artifact.part(part), "wb", compression=artifact.compression
        ) as fh:
            fh.write(statement_csv_header())
        return [part]

    def _pending_by_shard(
        self, session: ExportSession
    ) -> dict[str, dict[str, frozenset[str]]]:
        """Each diff series' DEL candidates per shard – not per pair: an id names
        its shard but not its bucket, and its live rows may sit in another
        bucket than its tombstones."""
        shards = self.entities.shards
        out: dict[str, dict[str, set[str]]] = {}
        for run in session.diffable:
            for entity_id in run.pending:
                shard = entity_shard(entity_id, shards)
                out.setdefault(shard, {}).setdefault(run.name, set()).add(entity_id)
        return {
            shard: {name: frozenset(ids) for name, ids in runs.items()}
            for shard, runs in out.items()
        }

    def export_index(self) -> None:
        """Write ``index.json``, registering what the exports produced."""
        dataset = self._model
        dataset.resources = list(self.artifacts.resources())
        statistics = self.artifacts.statistics
        if statistics.exists():
            dataset.apply_stats(self._store.get(statistics.key, model=DatasetStats))
        self.artifacts.index.write(dataset)

    def handle(self, run: JobRun[ExportJob], *args: Any, **kwargs: Any) -> None:
        """The sweep, then every artifact's freshness tag – none if it crashed –
        then ``index.json``, also for an empty store."""
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
