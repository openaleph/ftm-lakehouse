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
    dependencies = [tag.STATEMENTS_OPTIMIZED]
    """Only the canonical-content clock. Exports are a function of what the
    store canonically holds, and
    [`prepare`][MakeOperation.prepare] runs ahead of the freshness check, so
    outstanding journal rows cannot hide from a run – they are drained and
    merged first, and that moves
    [`STATEMENTS_OPTIMIZED`][ftm_lakehouse.core.conventions.tag.STATEMENTS_OPTIMIZED].
    Depending on ``journal/last_updated`` instead meant a continuously-fed
    dataset was never fresh, so a scheduled ``make`` never converged."""

    def prepare(self) -> None:
        """Drain the journal and merge – the run's only flush and merge pass.

        Ahead of the freshness window, as the base class requires: ``merge``
        stamps this operation's own dependency, so doing it from inside
        `handle` would backdate the target against it and leave ``make``
        permanently stale. The three exports `handle` runs are constructed
        ``prepared``, so none of them repeats this.
        """
        self.prepare_canonical()

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        """Run the export sweep, then the two artifacts computed from it."""
        force = kwargs.get("force", False)
        for kind in MAKE_KINDS:
            job = ExportJob.make(dataset=self.dataset, kind=kind)
            ExportOperation(job, self.uri, prepared=True).run(force=force)
        run.job.done = 1
