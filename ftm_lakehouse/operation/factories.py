"""Run operations on a dataset without constructing job and operation instances.

Example:
    ```python
    from ftm_lakehouse.operation import export, make, optimize

    # Write every export artifact from one sweep
    export("my_dataset")

    # Optimize the statement store (merge + vacuum)
    optimize("my_dataset")

    # Run the full make workflow (flush + export)
    make("my_dataset")
    ```
"""

from anystore.types import Uri

from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.operation.maintenance import (
    MigrateJob,
    MigrateOperation,
    OptimizeJob,
    OptimizeOperation,
    ShardJob,
    ShardOperation,
)
from ftm_lakehouse.operation.make import MakeJob, MakeOperation


def export(
    dataset: str,
    uri: Uri | None = None,
    force: bool = False,
    make_diff: bool = True,
) -> ExportJob:
    """Run the export: every artifact from one sweep, then ``index.json``.

    Compression is the dataset's ``compression`` config – deliberately no
    argument, so every writer and reader of a dataset agrees on the layout.
    """
    job = ExportJob.make(dataset=dataset, make_diff=make_diff)
    return ExportOperation(job, uri).run(force=force)


def optimize(
    dataset: str,
    uri: Uri | None = None,
    retention_hours: int = 0,
    force: bool = False,
) -> OptimizeJob:
    """Optimize the statement store: merge every dirty partition, then vacuum."""
    job = OptimizeJob.make(
        dataset=dataset,
        retention_hours=retention_hours,
    )
    return OptimizeOperation(job, uri).run(force=force)


def shard(
    dataset: str,
    shards: int,
    uri: Uri | None = None,
    force: bool = False,
) -> ShardJob:
    """Rewrite the statement store onto ``shards`` shards, then record the
    count in ``config.yml``.

    Run with writers stopped (journal writes are not fenced) and ``optimize``
    afterwards; ``force`` repairs a config changed without a rewrite.
    """
    job = ShardJob.make(dataset=dataset, shards=shards)
    return ShardOperation(job, uri).run(force=force)


def migrate(
    dataset: str,
    uri: Uri | None = None,
    force: bool = False,
) -> MigrateJob:
    """Apply the storage-layout migrations this dataset has not seen yet;
    ``force`` re-runs all of them (they are idempotent)."""
    job = MigrateJob.make(dataset=dataset)
    return MigrateOperation(job, uri).run(force=force)


def make(dataset: str, uri: Uri | None = None, force: bool = False) -> MakeJob:
    """Run the make workflow: flush the journal, then export."""
    job = MakeJob.make(dataset=dataset)
    return MakeOperation(job, uri).run(force=force)


def download_archive(
    dataset: str, target: Uri, uri: Uri | None = None
) -> DownloadArchiveJob:
    """Download the archive files to ``target`` under their original
    relative paths."""
    job = DownloadArchiveJob.make(dataset=dataset, target=target)
    return DownloadArchiveOperation(job, uri).run()
