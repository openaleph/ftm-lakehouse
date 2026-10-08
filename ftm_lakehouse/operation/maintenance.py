"""Statement-store maintenance: optimize (merge + vacuum), re-shard, migrate."""

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

    Merge rewrites each dirty ``(shard, bucket, origin)`` partition into one
    file – latest row per statement id, ``first_seen`` folded to the minimum,
    tombstones past the grace period dropped; vacuum deletes the replaced files.
    """

    target = tag.OP_OPTIMIZE
    dependencies: list[str] = []

    def is_fresh(self) -> bool:
        """Whether no partition is dirty – asked of the store
        ([`ParquetStore.needs_merge`][ftm_lakehouse.storage.parquet.ParquetStore.needs_merge]),
        not a tag pair."""
        return not self.entities.statements.needs_merge

    def handle(self, run: JobRun[OptimizeJob], force: bool = False, **kwargs) -> None:
        self.entities.merge(force)
        run.job.done += 1
        run.save()
        self.entities.statements.vacuum(retention_hours=run.job.retention_hours)
        run.job.done += 1


class ShardJob(DatasetJobModel):
    shards: int = Field(ge=0)
    """Target entity-id hash shard count; ``0`` / ``1`` means a single shard."""


class ShardOperation(DatasetJobOperation[ShardJob]):
    """Change the dataset's shard count: rewrite the store, then the config.

    The rewrite drains the journal and moves every ``(bucket, origin)`` group
    into the new shard partitions (one atomic Delta commit per group); then the
    count is written to ``config.yml`` and the factory caches are cleared. The
    config goes last – it declares the layout to every other process – so a run
    that dies in between is repaired by running it again (the rewrite is
    idempotent). Every partition comes out dirty: run ``optimize`` afterwards.
    """

    target = tag.OP_SHARD

    def is_fresh(self) -> bool:
        """Whether the config already names the target count – not a tag pair,
        so a hand-edited config reads as fresh; ``force`` overrides."""
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

    Runs ``ftm_lakehouse.operation.migrations.MIGRATIONS`` in order, stamping
    [`tag.migration`][ftm_lakehouse.core.conventions.tag.migration] per
    migration, so a run that dies halfway resumes at the first untagged one.
    ``force`` re-runs all – migrations are idempotent.
    """

    target = tag.OP_MIGRATE

    @property
    def outstanding(self) -> tuple[Migration, ...]:
        """The registered migrations this dataset carries no tag for."""
        return tuple(
            m for m in MIGRATIONS if self._tags.get(tag.migration(m.__name__)) is None
        )

    def is_fresh(self) -> bool:
        """Whether every registered migration has run – not a tag pair."""
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
