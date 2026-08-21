"""MakeOperation - full workflow: flush journal + all exports."""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
from ftm_lakehouse.repository.job import JobRun


class MakeJob(DatasetJobModel):
    pass


class MakeOperation(DatasetJobOperation[MakeJob]):
    target = tag.OP_MAKE
    dependencies = [tag.JOURNAL_UPDATED, tag.STATEMENTS_OPTIMIZED]

    def prepare(self) -> None:
        """Drain the journal; each export merges for itself in its own
        :meth:`ExportOperation.prepare`."""
        self.entities.flush()

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        force = kwargs.get("force", False)
        for kind in ExportKind:
            job = ExportJob.make(dataset=self.dataset, kind=kind)
            ExportOperation(job, self.uri).run(force=force)
        run.job.done = 1
