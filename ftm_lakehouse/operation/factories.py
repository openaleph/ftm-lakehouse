"""Factory functions for creating and running operations from a Dataset.

These factories provide a convenient way to run operations without manually
constructing Job and Operation instances.

Example:
    ```python
    from ftm_lakehouse import get_dataset
    from ftm_lakehouse.operation import export, make, optimize

    dataset = get_dataset("my_dataset")

    # Run a single export operation
    export(dataset, "statements")

    # Optimize the statement store (merge + compact + vacuum)
    optimize(dataset)

    # Run the full make workflow (flush + all exports)
    make(dataset)
    ```
"""

from anystore.types import Uri

from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
from ftm_lakehouse.operation.maintenance import OptimizeJob, OptimizeOperation
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation


def export(
    dataset: Dataset,
    kind: ExportKind | str,
    force: bool = False,
    make_diff: bool = True,
) -> ExportJob:
    """
    Run a single export operation.

    Args:
        dataset: The dataset to export from
        kind: What to export – one of ``statements``, ``entities``,
            ``documents``, ``statistics``, ``index``
        force: Force export even if up-to-date
        make_diff: Also export a delta diff file (``entities`` / ``documents``)

    Returns:
        The completed job result
    """
    job = ExportJob.make(
        dataset=dataset.name, kind=ExportKind(kind), make_diff=make_diff
    )
    return ExportOperation.from_job(job, dataset).run(force=force)


def optimize(
    dataset: Dataset,
    retention_hours: int = 0,
    grace_period_days: int | None = None,
    force: bool = False,
) -> OptimizeJob:
    """
    Optimize the statement store: merge duplicates / reap tombstones,
    bin-pack small files, delete obsolete files.

    Args:
        dataset: The dataset to optimize
        retention_hours: Vacuum retains obsolete files newer than this
        grace_period_days: Override ``LAKEHOUSE_GRACE_PERIOD_DAYS`` for merge
        force: Run regardless of freshness state

    Returns:
        The completed job result
    """
    job = OptimizeJob.make(
        dataset=dataset.name,
        retention_hours=retention_hours,
        grace_period_days=grace_period_days,
    )
    return OptimizeOperation.from_job(job, dataset).run(force=force)


def run_mapping(
    dataset: Dataset,
    content_hash: str,
    force: bool = False,
) -> MappingJob:
    """
    Run mapping operation to process a CSV file into entities.

    Args:
        dataset: The dataset containing the mapping
        content_hash: SHA256 checksum of the CSV file to process
        force: Force processing even if up-to-date

    Returns:
        The completed job result
    """
    job = MappingJob.make(dataset=dataset.name, content_hash=content_hash)
    return MappingOperation.from_job(job, dataset).run(force=force)


def make(dataset: Dataset, force: bool = False) -> MakeJob:
    """
    Run the full make workflow: flush journal and generate all exports.

    Args:
        dataset: The dataset to process
        force: Force all operations even if up-to-date

    Returns:
        The completed job result
    """
    job = MakeJob.make(dataset=dataset.name)
    return MakeOperation.from_job(job, dataset).run(force=force)


def download_archive(dataset: Dataset, target: Uri) -> DownloadArchiveJob:
    """
    Download (export) the archive files to a target, rewriting to original
    relative paths.

    Args:
        dataset: The dataset to process
        target: The uri to the target (local or remote)
    """
    job = DownloadArchiveJob.make(dataset=dataset.name, target=target)
    return DownloadArchiveOperation.from_job(job, dataset).run()
