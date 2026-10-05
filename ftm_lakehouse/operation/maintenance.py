"""Statement-store maintenance: optimize, re-shard and migrate.

[`OptimizeOperation`][OptimizeOperation] runs the two Delta Lake maintenance steps in
order – the use case is always both together:

1. merge – rewrite every dirty partition into one canonical file: collapse
   duplicates, fold ``first_seen``, reap tombstones past the grace period
2. vacuum – delete the files a merge replaced from disk

Reads reconcile un-merged rows, so this is an optimisation – a merged
partition is a plain scan – and the disk reclaim; run it after large write
batches.

[`ShardOperation`][ShardOperation] is the rarer one: it changes the dataset's shard
count, which means rewriting every partition and then recording the new
count in ``config.yml``.

[`MigrateOperation`][MigrateOperation] applies the storage-layout migrations a
dataset has not seen yet – the registry is
``ftm_lakehouse.operation.migrations``.
"""

from typing import Any

from anystore.util import Took
from pydantic import Field

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.migrations import MIGRATIONS, Migration
from ftm_lakehouse.repository import factories
from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.job import JobRun


class OptimizeJob(DatasetJobModel):
    retention_hours: int = 0
    """Vacuum: retain obsolete files newer than this many hours."""


class OptimizeOperation(DatasetJobOperation[OptimizeJob]):
    """Optimize the parquet statement store: merge, then vacuum.

    For each dirty ``(shard, bucket, origin)`` partition: keep the most-recent
    row per statement id, fold ``first_seen`` down to the minimum, drop
    tombstones older than the grace period – one file per partition – then
    delete the files that replaced. Each step is held under the dataset write
    fence.
    """

    target = tag.OP_OPTIMIZE
    dependencies: list[str] = []

    def is_fresh(self) -> bool:
        """Ask the statement store whether any partition is dirty.

        Not a tag pair: the store knows from its own file list which
        partitions hold rows a merge has not rewritten
        ([`ParquetStore.needs_merge`][ftm_lakehouse.storage.parquet.ParquetStore.needs_merge]),
        and that is the only thing an optimize has to do.
        """
        return not self.entities.needs_merge

    def handle(self, run: JobRun[OptimizeJob], force: bool = False, **kwargs) -> None:
        self.entities.merge(force)
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

    1. `shard`
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
    partition dirty – reads reconcile it; run ``optimize`` afterwards to get
    plain-scan reads and the file sort order back.
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


class MigrateJob(DatasetJobModel):
    """No parameters – a migrate run is always "everything outstanding"."""


class MigrateOperation(DatasetJobOperation[MigrateJob]):
    """Apply the storage-layout migrations this dataset has not seen yet.

    Runs the functions registered in ``ftm_lakehouse.operation.migrations`` in
    registry order, stamping each with
    [`tag.migration`][ftm_lakehouse.core.conventions.tag.migration] on
    completion. Per-migration tags rather than one version number: a run that
    dies halfway keeps what it finished and the next one picks up at the first
    untagged migration. ``force`` re-runs the whole registry – migrations are
    idempotent.
    """

    target = tag.OP_MIGRATE

    @property
    def outstanding(self) -> tuple[Migration, ...]:
        """The registered migrations this dataset carries no tag for."""
        return tuple(
            m for m in MIGRATIONS if self._tags.get(tag.migration(m.__name__)) is None
        )

    def is_fresh(self) -> bool:
        """Whether every registered migration has run against this dataset.

        Not a tag pair: a migration is done or not, and no dependency's
        timestamp can make an applied one stale again.
        """
        return not self.outstanding

    def handle(
        self, run: JobRun[MigrateJob], force: bool = False, **kwargs: Any
    ) -> None:
        migrations = MIGRATIONS if force else self.outstanding
        ref = DatasetRef(self.dataset, str(self.uri))
        run.job.pending = len(migrations)
        run.save()
        for migration in migrations:
            name = migration.__name__
            with self._tags.touch(tag.migration(name)), Took() as t:
                self.log.info(f"Running migration `{name}` ...", migration=name)
                migration(ref)
                run.job.pending -= 1
                run.job.done += 1
                run.save()
                self.log.info(f"Migration `{name}` done", migration=name, took=t.took)
