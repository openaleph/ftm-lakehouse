"""MakeOperation - full workflow: flush journal + all exports."""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.export import MAKE_KINDS, ExportJob, ExportOperation
from ftm_lakehouse.repository.job import JobRun


class MakeJob(DatasetJobModel):
    pass


class MakeOperation(DatasetJobOperation[MakeJob]):
    target = tag.OP_MAKE
    dependencies = [tag.JOURNAL_UPDATED, tag.STATEMENTS_OPTIMIZED]

    def prepare(self) -> None:
        """Drain the journal; each export merges for itself in its own
        [`ExportOperation.prepare`][ExportOperation.prepare]."""
        self.entities.flush()

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        """Run the export sweep, then the two artifacts computed from it."""
        force = kwargs.get("force", False)
        for kind in MAKE_KINDS:
            job = ExportJob.make(dataset=self.dataset, kind=kind)
            ExportOperation(job, self.uri).run(force=force)
        run.job.done = 1
