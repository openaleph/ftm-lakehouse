"""MakeOperation - full workflow: flush journal + all exports."""

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation import factories
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class MakeJob(DatasetJobModel):
    pass


class MakeOperation(DatasetJobOperation[MakeJob]):
    target = tag.OP_MAKE
    dependencies = [tag.JOURNAL_UPDATED, tag.STATEMENTS_UPDATED]

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        force = kwargs.get("force", False)
        ds = self._dataset
        ds.entities.flush()
        factories.export_statements(ds, force=force)
        factories.export_entities(ds, force=force)
        factories.export_documents(ds, force=force)
        factories.export_statistics(ds, force=force)
        factories.export_index(ds, force=force)
        run.job.done = 1
