"""Statement-store maintenance: optimize and re-shard.

:class:`OptimizeOperation` runs the three Delta Lake maintenance steps in
order – the use case is always all of them together:

1. merge – collapse duplicates / fold ``first_seen`` / reap tombstones
   past the grace period
2. compact – bin-pack small parquet files
3. vacuum – delete obsolete parquet files from disk

Exports and statistics assume an optimized store, so run this after large
write batches.

:class:`ShardOperation` is the rarer one: it changes the dataset's shard
count, which means rewriting every partition and then recording the new
count in ``config.yml``.
"""

from typing import Any

from pydantic import Field

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository import factories
from ftm_lakehouse.repository.job import JobRun


class OptimizeJob(DatasetJobModel):
    retention_hours: int = 0
    """Vacuum: retain obsolete files newer than this many hours."""


class OptimizeOperation(DatasetJobOperation[OptimizeJob]):
    """Optimize the parquet statement store: merge, compact, vacuum.

    For each ``(shard, bucket, origin)`` partition: keep the most-recent row
    per statement id, fold ``first_seen`` down to the minimum, drop tombstones
    older than the grace period – then bin-pack small files and delete
    obsolete ones. Each step is held under the dataset write fence.
    """

    target = tag.STATEMENTS_OPTIMIZED
    dependencies = [tag.STATEMENTS_UPDATED]

    def is_fresh(self) -> bool:
        """Ask the statement store whether any partition is unmerged.

        The tag pair cannot answer this one. ``merge`` stamps
        :data:`~ftm_lakehouse.core.conventions.tag.STATEMENTS_OPTIMIZED` on
        completion while the target tag records when this operation *started*,
        so a successful optimize always finishes behind its own dependency and
        reads as stale – costing a redundant full pass every time. The
        per-partition tags :meth:`ParquetStore.merge` compares internally are
        the sound predicate, and ``needs_merge`` is that comparison.
        """
        return not self.entities.needs_merge

    def handle(self, run: JobRun[OptimizeJob], force: bool = False, **kwargs) -> None:
        self.entities.merge(force)
        run.job.done += 1
        run.save()
        self.entities.compact()
        run.job.done += 1
        run.save()
        self.entities.vacuum(retention_hours=run.job.retention_hours)
        run.job.done += 1


class ShardJob(DatasetJobModel):
    shards: int = Field(ge=0)
    """Target number of entity-id hash shards. ``0`` / ``1`` means a single
    shard; the value is bounded below because it becomes a partition key."""


class ShardOperation(DatasetJobOperation[ShardJob]):
    """Change the dataset's shard count: rewrite the store, then the config.

    The shard count is otherwise fixed at creation – every reader and
    writer resolves it from ``config.yml`` – so growing it is a full
    rewrite of the statement store. The typical trigger is a dataset that
    outgrew its layout: one shard means one partition per
    ``(bucket, origin)``, and queries that have to scan it whole get
    slow.

    Two steps, in this order:

    1. :meth:`~ftm_lakehouse.repository.entities.main.EntityRepository.shard`
       drains the journal and rewrites every ``(bucket, origin)`` group
       into the new shard partitions, streamed, one atomic Delta commit
       per group.
    2. the new count is written to ``config.yml`` (versioned like every
       other config write) and the repository factory caches are
       invalidated, so repositories fetched afterwards resolve the new
       layout.

    The config write goes last on purpose: it is what declares the layout
    to every other process, so it must not run ahead of the data. A run
    that dies in between leaves the config on the old count and is
    repaired by running it again – the rewrite recomputes each shard from
    ``entity_id`` alone, so it is idempotent.

    The rewrite is neither sorted nor deduped, which leaves every
    partition marked dirty – run ``optimize`` afterwards to restore
    canonical content and file sort order.
    """

    target = tag.OP_SHARD

    def is_fresh(self) -> bool:
        """Whether the dataset is already configured for the target count.

        Not a tag pair: what a re-shard changes is the configured layout,
        so the config *is* the freshness state. Consequently a config
        edited by hand to a count the store was never rewritten for reads
        as fresh – ``force`` is the way out of that.
        """
        return self._model.shards == self.job.shards

    def handle(self, run: JobRun[ShardJob], **kwargs: Any) -> None:
        self.entities.shard(self.job.shards)
        run.job.done += 1
        run.save()
        self._versions.make(
            path.CONFIG, self._model.model_copy(update={"shards": self.job.shards})
        )
        factories.clear_caches()
        run.job.done += 1
