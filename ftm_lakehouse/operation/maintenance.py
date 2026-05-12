"""Async maintenance operations on the parquet statement store.

Three independent jobs, each acquiring the anystore Lock at
``path.lock(<op>)`` so concurrent invocations on the same dataset serialize:

- ``CompactJob`` — bin-pack small parquet files (cheap, run often).
- ``MergeJob`` — collapse duplicates / fold ``first_seen`` / reap tombstones
  past the grace period (expensive, run sparingly).
- ``VacuumJob`` — delete obsolete parquet files from disk (run daily).
"""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class CompactJob(DatasetJobModel):
    pass


class MergeJob(DatasetJobModel):
    pass


class VacuumJob(DatasetJobModel):
    retention_hours: int = 0


class CompactOperation(DatasetJobOperation[CompactJob]):
    """Bin-pack small parquet files (Delta OPTIMIZE compact).

    Cheap maintenance — only rewrites small files into larger ones; does not
    dedupe rows or drop tombstones (use ``MergeOperation`` for that).
    """

    target = tag.STATEMENTS_COMPACTED
    dependencies = [tag.STATEMENTS_UPDATED]

    def handle(self, run: JobRun[CompactJob], *args, **kwargs) -> None:
        self.entities._statements.compact()
        run.job.done = 1


class MergeOperation(DatasetJobOperation[MergeJob]):
    """Collapse duplicates and reap expired tombstones, partition by partition.

    For each ``(shard, bucket, origin)`` partition: keep the most-recent row
    per statement id, fold ``first_seen`` down to the minimum, drop tombstones
    older than ``LAKEHOUSE_GRACE_PERIOD_DAYS``.
    """

    target = tag.STATEMENTS_MERGED
    dependencies = [tag.STATEMENTS_UPDATED]

    def handle(self, run: JobRun[MergeJob], *args, **kwargs) -> None:
        self.entities._statements.merge()
        run.job.done = 1


class VacuumOperation(DatasetJobOperation[VacuumJob]):
    """Delete obsolete parquet files no longer referenced by the Delta log."""

    target = tag.STATEMENTS_VACUUMED
    dependencies = [tag.STATEMENTS_COMPACTED, tag.STATEMENTS_MERGED]

    def handle(self, run: JobRun[VacuumJob], *args, **kwargs) -> None:
        self.entities._statements.vacuum(retention_hours=run.job.retention_hours)
        run.job.done = 1
