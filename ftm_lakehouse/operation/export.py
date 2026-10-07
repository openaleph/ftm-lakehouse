"""Export operations (parquet -> statements.csv, entities.ftm.json,
documents.csv, statistics.json, index.json) plus their diff series.

Every artifact that is a function of the entity stream is written in **one
sweep**: `ExportOperation.export` scans the statement store once, pair by
pair, folds the rows into entities and hands each one to every artifact.

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
    """One ``(shard, bucket)`` pair's sweep as handed to a worker.

    Plain data, so it pickles, and nothing in it reaches the Delta log: the
    parent resolves the pair against **one** snapshot and ships the relation
    SQL, so every worker reads the same version of the store and none of them
    replays a 15GB transaction log to find out which files to read.
    """

    dataset: str
    uri: str
    now: datetime
    version: int | None
    make_diff: bool
    parts: str
    shard: str
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

    The whole fan-out, unchanged – the artifacts are the same classes writing
    the same formats through the same codec, pointed at a part directory
    instead of at the artifact keys
    ([`Artifact.part`][ftm_lakehouse.repository.artifacts.Artifact.part]).

    Prepares and closes its session but never finishes or commits it: every
    `ArtifactRun.finish` is work over the whole store – the folder tree spans
    shards, the statistics are a fold of every entity, a ``DEL`` is what no
    worker met alive – and a commit writes tags, which is the parent's.

    Deliberately silent, like
    [`merge_partition`][ftm_lakehouse.storage.parquet.merge_partition]: a
    spawned worker re-imports the library without the CLI's logging setup, so
    ``took`` travels back in the `ExportPart` for the parent to log.

    Args:
        task: The pair, its relation, and the window and candidates its diff
            series need.

    Returns:
        `ExportPart` – the counts, the candidates met alive, the statistics.
    """
    with Took() as t:
        artifacts = get_artifacts(task.dataset, task.uri)
        # built directly rather than through `ArtifactsRepository.session`,
        # which wires the delete-candidate scan: that is one pass over the
        # whole store and the parent has already run it
        runs = tuple(a.run(task.now, task.parts) for a in artifacts.streamed())
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

    def export(self, now: datetime) -> dict[str, int]:
        """Write every streamed artifact from one pass over the entities.

        Each ``(shard, bucket)`` pair is swept into parts of every artifact –
        by ``LAKEHOUSE_WORKERS`` processes, or in this one – and the parts are
        concatenated. The parent keeps what cannot be partitioned: the
        snapshot every pair is read from, the delete-candidate scan, the folder
        tree, the statistics merge and the tags.

        Held under the statement store's merge lock: an ``optimize`` would
        vacuum the files the snapshot names. Appends are not affected.

        Args:
            now: Timestamp the run started – the diff files are named after it
                and the diff states are recorded at it.

        Returns:
            Counts per artifact and per diff op.
        """
        workers = max(settings.workers, 1)
        config = worker_duckdb_config(workers)
        store = self.entities._statements
        counts: Counter[str] = Counter()
        with (
            self.entities.merge_lock(),
            TemporaryDirectory(prefix="ftm-lakehouse-export-") as tmp,
        ):
            version = store.version
            sources = store.sweep_sources()
            # the parent writes its own part – the documents csv and the DEL
            # rows its `finish` produces belong in the assembled file too
            session = self.artifacts.session(
                now, version, f"{tmp}/parent", self.job.make_diff
            )
            parts = [f"{tmp}/{shard}-{bucket}" for (shard, bucket), _, _ in sources]
            with (
                session,
                SyncProgressBar("Exporting statements", store.num_rows) as bar,
                process_map(workers) as run,
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
                        source=source,
                        clean=clean,
                        duckdb_config=config,
                        pending=pending.get(shard, {}),
                    )
                    for part, ((shard, bucket), source, clean) in zip(parts, sources)
                ]
                for task, part in zip(tasks, run(export_partition, tasks)):
                    counts.update(part.counts)
                    session.adopt(part.parts, part.seen, part.stats)
                    bar.advance(part.counts.get("statements", 0))
                    self.log.info(
                        f"Swept pair `{task.shard}`.",
                        took=part.took,
                        shard=task.shard,
                        statements=part.counts.get("statements", 0),
                    )
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
        """The ``statements.csv`` header, as the first part of the assembly.

        The workers write headerless parts, so the header is its own piece –
        written through the dataset's codec, so the assembly stays a verbatim
        copy of frames. A run that swept nothing writes none, so an empty
        export's csv stays empty.

        Args:
            tmp: The run's part root.
            statements: Statements the run swept.

        Returns:
            The header's part directory, or nothing when there was no row.
        """
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
        """Each diff series' DEL candidates, split by the shard they live in.

        A worker can only claim an id it meets alive, and it only ever meets
        ids of its own pair – so it is handed its shard's candidates and
        nothing else. By **shard**, not by pair: an id determines its shard but
        not its bucket, and an entity's live rows may sit in a different bucket
        than its tombstoned ones, so narrowing any further would turn a missed
        claim into a ``DEL`` for an entity that is still there.

        Args:
            session: The parent's session, its candidates already loaded.

        Returns:
            Per shard, per run name, the candidate ids to hand that worker.
        """
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
