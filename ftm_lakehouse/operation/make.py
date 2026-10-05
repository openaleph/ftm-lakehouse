"""MakeOperation - full workflow: flush journal + all exports."""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.export import MAKE_KINDS, ExportJob, ExportOperation
from ftm_lakehouse.repository.job import JobRun


class MakeJob(DatasetJobModel):
    pass


class MakeOperation(DatasetJobOperation[MakeJob]):
    """Flush the journal and run every export kind.

    Never merges: reads reconcile un-merged rows, so the exports are correct
    on any store. Merging is
    [`OptimizeOperation`][ftm_lakehouse.operation.maintenance.OptimizeOperation]'s
    business – the ``make`` CLI runs it first by default, as an optimisation.
    """

    target = tag.OP_MAKE
    dependencies = [tag.STATEMENTS_UPDATED]
    """The content clock. [`prepare`][MakeOperation.prepare] runs ahead of the
    freshness check, so rows still in the journal cannot hide from a run –
    they are drained first, and a drain that lands rows moves this tag."""

    def prepare(self) -> None:
        """Drain the journal – a ``LIMIT 1`` probe when it is empty."""
        self.entities.flush()

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        """Run the export sweep, then the two artifacts computed from it."""
        force = kwargs.get("force", False)
        for kind in MAKE_KINDS:
            job = ExportJob.make(dataset=self.dataset, kind=kind)
            ExportOperation(job, self.uri).run(force=force)
        run.job.done = 1
