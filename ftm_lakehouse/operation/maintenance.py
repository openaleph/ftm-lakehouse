"""Statement-store optimization.

One operation runs the three Delta Lake maintenance steps in order – the
use case is always all of them together:

1. merge – collapse duplicates / fold ``first_seen`` / reap tombstones
   past the grace period
2. compact – bin-pack small parquet files
3. vacuum – delete obsolete parquet files from disk

Exports and statistics assume an optimized store, so run this after large
write batches.
"""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class OptimizeJob(DatasetJobModel):
    retention_hours: int = 0
    """Vacuum: retain obsolete files newer than this many hours."""
    grace_period_days: int | None = None
    """Merge: override ``LAKEHOUSE_GRACE_PERIOD_DAYS`` for tombstone reaping."""


class OptimizeOperation(DatasetJobOperation[OptimizeJob]):
    """Optimize the parquet statement store: merge, compact, vacuum.

    For each ``(shard, bucket, origin)`` partition: keep the most-recent row
    per statement id, fold ``first_seen`` down to the minimum, drop tombstones
    older than the grace period – then bin-pack small files and delete
    obsolete ones. Each step is held under the dataset write fence.
    """

    target = tag.STATEMENTS_OPTIMIZED
    dependencies = [tag.STATEMENTS_UPDATED]

    def handle(self, run: JobRun[OptimizeJob], *args, **kwargs) -> None:
        store = self.entities._statements
        store.merge(run.job.grace_period_days)
        run.job.done += 1
        run.save()
        store.compact()
        run.job.done += 1
        run.save()
        store.vacuum(retention_hours=run.job.retention_hours)
        run.job.done += 1
