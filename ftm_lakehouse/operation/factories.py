"""Factory functions for creating and running operations from a Dataset.

These factories provide a convenient way to run operations without manually
constructing Job and Operation instances.

Example:
    ```python
    from ftm_lakehouse import get_dataset
    from ftm_lakehouse.operation import export_statements, make

    dataset = get_dataset("my_dataset")

    # Run a single export operation
    export_statements(dataset)

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
from ftm_lakehouse.operation.export import (
    ExportDocumentsJob,
    ExportDocumentsOperation,
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.operation.optimize import OptimizeJob, OptimizeOperation
from ftm_lakehouse.operation.recreate import (
    RecreateJob,
    RecreateOperation,
    RecreateSource,
)


def export_statements(dataset: Dataset, force: bool = False) -> ExportStatementsJob:
    """
    Run export statements operation (parquet -> statements.csv).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    job = ExportStatementsJob.make(dataset=dataset.name)
    return ExportStatementsOperation.from_job(job, dataset).run(force=force)


def export_entities(
    dataset: Dataset, force: bool = False, make_diff: bool = True
) -> ExportEntitiesJob:
    """
    Run export entities operation (parquet -> entities.ftm.json).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date
        make_diff: Also export delta diff file (default True)

    Returns:
        The completed job result
    """
    job = ExportEntitiesJob.make(dataset=dataset.name, make_diff=make_diff)
    return ExportEntitiesOperation.from_job(job, dataset).run(force=force)


def export_statistics(dataset: Dataset, force: bool = False) -> ExportStatisticsJob:
    """
    Run export statistics operation (parquet -> statistics.json).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    job = ExportStatisticsJob.make(dataset=dataset.name)
    return ExportStatisticsOperation.from_job(job, dataset).run(force=force)


def export_documents(
    dataset: Dataset, force: bool = False, make_diff: bool = True
) -> ExportDocumentsJob:
    """
    Run export documents operation (parquet -> documents.csv).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    job = ExportDocumentsJob.make(dataset=dataset.name, make_diff=make_diff)
    return ExportDocumentsOperation.from_job(job, dataset).run(force=force)


def export_index(dataset: Dataset, force: bool = False) -> ExportIndexJob:
    """
    Run export index operation (-> index.json).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    job = ExportIndexJob.make(dataset=dataset.name)
    return ExportIndexOperation.from_job(job, dataset).run(
        force=force, dataset=dataset.model
    )


def optimize(
    dataset: Dataset,
    vacuum: bool = False,
    vacuum_keep_hours: int = 0,
    bucket: str | None = None,
    origin: str | None = None,
    force: bool = False,
    compact: bool = False,
) -> OptimizeJob:
    """
    Run optimize operation on the parquet statement store.

    Args:
        dataset: The dataset to optimize
        vacuum: Delete stale files after optimization
        vacuum_keep_hours: Keep files newer than this many hours
        bucket: Scope optimization to a specific bucket
        origin: Scope optimization to a specific origin
        force: Force optimization even if up-to-date
        compact: Dedupe statements and clear out delete tombstones

    Returns:
        The completed job result
    """
    job = OptimizeJob.make(
        dataset=dataset.name,
        vacuum=vacuum,
        vacuum_keep_hours=vacuum_keep_hours,
        bucket=bucket,
        origin=origin,
        compact=compact,
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


def recreate(
    dataset: Dataset, source: RecreateSource = RecreateSource.AUTO
) -> RecreateJob:
    """
    Recreate a corrupted dataset by rebuilding the parquet store from exports.

    This operation repairs corrupted lakehouse datasets by clearing the
    statement store (parquet), then re-importing from the most
    recent export file (entities.ftm.json or statements.csv).

    Warning: This operation is destructive - it will delete all existing
    statement data before re-importing from exports.

    Args:
        dataset: The dataset to recreate
        source: Source for recreation (AUTO selects based on timestamps)

    Returns:
        The completed job result
    """
    job = RecreateJob.make(dataset=dataset.name, source=source)
    return RecreateOperation.from_job(job, dataset).run()


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
