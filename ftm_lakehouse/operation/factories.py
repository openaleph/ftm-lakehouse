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

from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.operation.export import (
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.operation.optimize import OptimizeJob, OptimizeOperation


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
    op = ExportStatementsOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    return op.run(force=force)


def export_entities(dataset: Dataset, force: bool = False) -> ExportEntitiesJob:
    """
    Run export entities operation (parquet -> entities.ftm.json).

    Args:
        dataset: The dataset to export from
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    job = ExportEntitiesJob.make(dataset=dataset.name)
    op = ExportEntitiesOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    return op.run(force=force)


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
    op = ExportStatisticsOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    return op.run(force=force)


def export_index(
    dataset: Dataset,
    include_statements_csv: bool = False,
    include_entities_json: bool = False,
    include_statistics: bool = False,
    include_all: bool = False,
    force: bool = False,
) -> ExportIndexJob:
    """
    Run export index operation (-> index.json).

    Args:
        dataset: The dataset to export from
        include_statements_csv: Include statements.csv in index resources
        include_entities_json: Include entities.ftm.json in index resources
        include_statistics: Include statistics.json in index resources
        include_all: Shorthand to include all exports in index resources
        force: Force export even if up-to-date

    Returns:
        The completed job result
    """
    if include_all or force:
        include_statements_csv = True
        include_entities_json = True
        include_statistics = True

    job = ExportIndexJob.make(
        dataset=dataset.name,
        include_statements_csv=include_statements_csv,
        include_entities_json=include_entities_json,
        include_statistics=include_statistics,
    )
    op = ExportIndexOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    return op.run(dataset=dataset.model, force=force)


def optimize(
    dataset: Dataset,
    vacuum: bool = False,
    vacuum_keep_hours: int = 0,
    bucket: str | None = None,
    origin: str | None = None,
    force: bool = False,
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

    Returns:
        The completed job result
    """
    job = OptimizeJob.make(
        dataset=dataset.name,
        vacuum=vacuum,
        vacuum_keep_hours=vacuum_keep_hours,
        bucket=bucket,
        origin=origin,
    )
    op = OptimizeOperation(
        job=job,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset.entities._tags,
        versions=dataset.entities._versions,
    )
    return op.run(force=force)


def run_mapping(
    dataset: Dataset,
    content_hash: str,
    force: bool = False,
) -> MappingJob:
    """
    Run mapping operation to process a CSV file into entities.

    Args:
        dataset: The dataset containing the mapping
        content_hash: SHA1 checksum of the CSV file to process
        force: Force processing even if up-to-date

    Returns:
        The completed job result
    """
    job = MappingJob.make(dataset=dataset.name, content_hash=content_hash)
    op = MappingOperation(
        job=job,
        archive=dataset.archive,
        entities=dataset.entities,
        jobs=dataset.jobs,
        tags=dataset._tags,
        versions=dataset._versions,
    )
    return op.run(force=force)


def make(dataset: Dataset, with_resources: bool = True, force: bool = False) -> None:
    """
    Run the full make workflow: flush journal and generate all exports.

    Args:
        dataset: The dataset to process
        with_resources: Include all exports in index.json resources
        force: Force all operations even if up-to-date
    """
    dataset.entities.flush()
    export_index(dataset, include_all=with_resources, force=force)
