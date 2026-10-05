"""MakeOperation - full workflow: flush journal + all exports."""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.repository.job import JobRun


class MakeJob(DatasetJobModel):
    pass


class MakeOperation(DatasetJobOperation[MakeJob]):
    """Flush the journal and run the export.

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
        """Run the export – one sweep over every artifact, then ``index.json``."""
        job = ExportJob.make(dataset=self.dataset)
        ExportOperation(job, self.uri).run(force=kwargs.get("force", False))
        run.job.done = 1
