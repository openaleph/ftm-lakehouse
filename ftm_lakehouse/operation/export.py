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

import multiprocessing
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Iterator

from anystore.io import SyncProgressBar, smart_open
from anystore.io.progress import Throughput
from anystore.util import Took, mask_uri
from ftmq.model.stats import DatasetStats
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.entities.aggregate import EntityPayload, aggregate_unsafe
from ftm_lakehouse.logic.entities.stats import StatsCollector
from ftm_lakehouse.logic.parquet import worker_duckdb_config
from ftm_lakehouse.model.dataset import DatasetModel, get_model_class, set_model_class
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
    model_class: str | None


@dataclass
class ExportPart:
    """What one worker produced, as the parent needs it back."""

    parts: str
    counts: dict[str, int]
    seen: dict[str, frozenset[str]]
    stats: StatsCollector
    took: timedelta


def _model_class_path() -> str | None:
    """The dotted path of the registered `DatasetModel` subclass, if any.

    ``None`` when the default is in use, so the common case ships nothing.

    Raises:
        RuntimeError: The registered class is not importable by path – a
            class defined inside a function is how the docs and the tests
            demonstrate `set_model_class`, and a worker could not resolve it.
            Without this check it would surface as a ``BrokenProcessPool``.
    """
    cls = get_model_class()
    if cls is DatasetModel:
        return None
    dotted = f"{cls.__module__}.{cls.__qualname__}"
    module, _, name = dotted.rpartition(".")
    try:
        resolved = getattr(import_module(module), name)
    except (ImportError, AttributeError):
        resolved = None
    if resolved is not cls:
        raise RuntimeError(
            f"Registered dataset model `{dotted}` is not importable, so a "
            "worker process cannot register it. Define it at module level, or "
            "export with `LAKEHOUSE_WORKERS=1`."
        )
    return dotted


def init_worker(model_class: str | None) -> None:
    """Re-register the parent's `DatasetModel` subclass in a fresh interpreter.

    ``set_model_class`` is a process-wide global and a spawned worker is a
    fresh import, so without this a worker parses ``config.yml`` into the base
    `DatasetModel` – which drops unknown keys silently rather than failing, so
    the divergence would reach the published artifacts unannounced.

    Args:
        model_class: Dotted path of the class to register, or ``None`` when
            the parent is using the default.
    """
    if model_class is None:
        return
    module, _, name = model_class.rpartition(".")
    set_model_class(getattr(import_module(module), name))


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
                    rows = sweep_partition(cur, csv, header=False)
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
            workers = max(settings.workers, 1)
            if workers > 1:
                return self.export_parallel(now, version, workers)
            session = self.artifacts.session(now, version, self.job.make_diff)
            count = self.entities._statements.num_rows
            # advanced per statement folded; its throughput is the Arrow
            # bytes the scan pulls from the store
            with session, SyncProgressBar("Exporting statements", count) as bar:
                for payload in self.iterate(bar.throughput):
                    session.consume(payload)
                    bar.advance(len(payload.statements))
            return session.result()

    def export_parallel(
        self, now: datetime, version: int | None, workers: int
    ) -> dict[str, int]:
        """The same export, one worker process per ``(shard, bucket)`` pair.

        The sweep is a single Python thread holding the GIL –
        ``to_pylist`` into the fold into the artifacts – so on a many-cored
        host it is the wall whatever the storage does. The pairs are
        independent (an entity id is placed in exactly one), so each worker
        folds its own entities into **parts** of every artifact and the parent
        concatenates them.

        What the parent keeps is what cannot be partitioned: one snapshot (so
        every worker reads the same version), the one delete-candidate scan,
        the folder tree (a document's ancestors are placed by their own ids,
        so they sit in other workers' pairs), the statistics fold, and every
        tag write.

        Args:
            now: Timestamp the run started.
            version: The pinned snapshot's Delta version.
            workers: Processes to fan the pairs out to.

        Returns:
            Counts per artifact and per diff op, summed over the parts – every
            value `ExportSession.result` reports is additive.
        """
        store = self.entities._statements
        sources = store.sweep_sources()
        config = worker_duckdb_config(workers)
        model_class = _model_class_path()
        counts: Counter[str] = Counter()
        with TemporaryDirectory(prefix="ftm-lakehouse-export-") as tmp:
            # the parent writes its own part – the documents csv and the DEL
            # rows its `finish` produces belong in the assembled file too
            session = self.artifacts.session(
                now, version, self.job.make_diff, f"{tmp}/parent"
            )
            parts = [f"{tmp}/{shard}-{bucket}" for (shard, bucket), _, _ in sources]
            with (
                session,
                SyncProgressBar("Exporting statements", store.num_rows) as bar,
                self._export_runner(workers, model_class) as run,
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
                        model_class=model_class,
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
        copy of frames. A run that swept nothing writes none, which keeps an
        empty export's csv empty, as the serial path leaves it.

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

    @staticmethod
    @contextmanager
    def _export_runner(
        workers: int, model_class: str | None
    ) -> Iterator[Callable[..., Iterator[Any]]]:
        """An ordered ``map`` over ``workers`` processes.

        Spawned rather than forked: the parent holds a ``DeltaTable`` with its
        own threads, a journal connection and a live progress-bar thread, none
        of which survive a fork intact. A spawned worker is a fresh import, so
        `init_worker` re-registers the dataset model class the parent was
        using. Pending pairs are cancelled when the run fails, instead of
        sweeping partitions whose parts nobody will assemble.
        """
        context = multiprocessing.get_context("spawn")
        pool = ProcessPoolExecutor(
            workers,
            mp_context=context,
            initializer=init_worker,
            initargs=(model_class,),
        )
        try:
            yield pool.map
        finally:
            pool.shutdown(cancel_futures=True)

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
