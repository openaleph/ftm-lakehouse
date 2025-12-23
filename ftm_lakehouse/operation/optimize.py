"""Optimize the parquet storage lake"""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class OptimizeJob(DatasetJobModel):
    bucket: str | None = None
    origin: str | None = None
    vacuum: bool = False
    vacuum_keep_hours: int = 0


class OptimizeOperation(DatasetJobOperation[OptimizeJob]):
    """
    Optimize the parquet delta like with optional vacuum (purge of old
    files). The optimization can be scoped to a bucket and/or an origin. For
    instance, after a crawl operation, only optimizing origin=crawl is
    feasible.

    Depending on the size of the dataset, this can be a very long running
    operation that may require some local memory and tmp disk storage.
    """

    target = tag.STORE_OPTIMIZED
    dependencies = [tag.STATEMENTS_UPDATED]

    def handle(self, run: JobRun[OptimizeJob], *args, **kwargs) -> None:
        self.entities._statements.optimize(
            vacuum=run.job.vacuum,
            vacuum_keep_hours=run.job.vacuum_keep_hours,
            bucket=run.job.bucket,
            origin=run.job.origin,
        )
        run.job.done = 1
