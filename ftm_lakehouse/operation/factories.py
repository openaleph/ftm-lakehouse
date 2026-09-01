"""Factory functions for creating and running operations on a dataset.

These factories provide a convenient way to run operations without manually
constructing Job and Operation instances.

Example:
    ```python
    from ftm_lakehouse.operation import export, make, optimize

    # Run a single export operation
    export("my_dataset", "statements")

    # Optimize the statement store (merge + compact + vacuum)
    optimize("my_dataset")

    # Run the full make workflow (flush + all exports)
    make("my_dataset")
    ```
"""

from anystore.types import Uri

from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
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
    kind: ExportKind | str,
    uri: Uri | None = None,
    force: bool = False,
    make_diff: bool = True,
) -> ExportJob:
    """
    Run a single export operation.

    Compression of the exported artifacts is the dataset's own
    ``compression`` config value – there is deliberately no runtime
    argument, so every writer and reader of a dataset agrees on the layout.

    Args:
        dataset: Name of the dataset to export from
        kind: What to export – one of ``statements``, ``entities``,
            ``documents``, ``statistics``, ``index``
        uri: Dataset storage root override
        force: Force export even if up-to-date
        make_diff: Also export a delta diff file (``entities`` / ``documents``)

    Returns:
        The completed job result
    """
    job = ExportJob.make(
        dataset=dataset,
        kind=ExportKind(kind),
        make_diff=make_diff,
    )
    return ExportOperation(job, uri).run(force=force)


def optimize(
    dataset: str,
    uri: Uri | None = None,
    retention_hours: int = 0,
    force: bool = False,
) -> OptimizeJob:
    """
    Optimize the statement store: merge duplicates / reap tombstones,
    bin-pack small files, delete obsolete files.

    Args:
        dataset: Name of the dataset to optimize
        uri: Dataset storage root override
        retention_hours: Vacuum retains obsolete files newer than this
        force: Run regardless of freshness state

    Returns:
        The completed job result
    """
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
    """
    Change the dataset's shard count: rewrite the statement store onto
    ``shards`` entity-hash shards, then record the count in ``config.yml``.

    A full rewrite of the store – the shard count is otherwise fixed at
    creation. Drains the journal first and leaves every partition marked
    dirty, so run ``optimize`` afterwards. Run with writers stopped: the
    write fence covers parquet appends, not journal writes.

    Args:
        dataset: Name of the dataset to re-shard
        shards: Target shard count (``0`` / ``1`` = a single shard)
        uri: Dataset storage root override
        force: Re-shard even when the dataset is already configured for
            ``shards`` – the way to repair a config that was changed
            without a rewrite

    Returns:
        The completed job result
    """
    job = ShardJob.make(dataset=dataset, shards=shards)
    return ShardOperation(job, uri).run(force=force)


def migrate(
    dataset: str,
    uri: Uri | None = None,
    force: bool = False,
) -> MigrateJob:
    """
    Apply the storage-layout migrations this dataset has not seen yet.

    Each migration is stamped with its own tag on completion, so this is a
    cheap no-op on an up-to-date dataset. See
    ``ftm_lakehouse.operation.migrations`` for the registry.

    Args:
        dataset: Name of the dataset to migrate
        uri: Dataset storage root override
        force: Re-run every registered migration, applied or not – they are
            idempotent

    Returns:
        The completed job result
    """
    job = MigrateJob.make(dataset=dataset)
    return MigrateOperation(job, uri).run(force=force)


def make(dataset: str, uri: Uri | None = None, force: bool = False) -> MakeJob:
    """
    Run the full make workflow: flush journal and generate all exports.

    Args:
        dataset: Name of the dataset to process
        uri: Dataset storage root override
        force: Force all operations even if up-to-date

    Returns:
        The completed job result
    """
    job = MakeJob.make(dataset=dataset)
    return MakeOperation(job, uri).run(force=force)


def download_archive(
    dataset: str, target: Uri, uri: Uri | None = None
) -> DownloadArchiveJob:
    """
    Download (export) the archive files to a target, rewriting to original
    relative paths.

    Args:
        dataset: Name of the dataset to process
        target: The uri to the target (local or remote)
        uri: Dataset storage root override
    """
    job = DownloadArchiveJob.make(dataset=dataset, target=target)
    return DownloadArchiveOperation(job, uri).run()
