from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Self

from anystore.types import Uri

from ftm_lakehouse.core.api import LakehouseApiMixin, api_delegate, require_api
from ftm_lakehouse.model.job import DJ
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_documents,
    get_entities,
    get_jobs,
    get_tags,
    get_versions,
)
from ftm_lakehouse.repository.job import JobRun

if TYPE_CHECKING:
    from ftm_lakehouse.dataset import Dataset


class DatasetJobOperation(LakehouseApiMixin, Generic[DJ]):
    """
    A (long-running) operation for a specific dataset that updates tags and
    checks dependencies for freshness to be able to skip this operation. The job
    result is stored after successful run.

    Repositories are resolved through the LRU-cached factories, so an
    operation shares its repository instances with every other path that
    addresses the same dataset.

    Subclasses can either set class attributes `target` and `dependencies`,
    or override `get_target()` and `get_dependencies()` for dynamic values.
    """

    target: str = ""  # tag that gets touched after successful run
    dependencies: list[str] = []  # dependencies for freshness check
    _dataset: Dataset

    def __init__(self, job: DJ, uri: Uri | None = None) -> None:
        self.job = job
        self.log = job.log
        self.archive = get_archive(job.dataset, uri)
        self.entities = get_entities(job.dataset, uri)
        self.documents = get_documents(job.dataset, uri)
        self.jobs = get_jobs(job.dataset, job.__class__, uri)
        self.tags = get_tags(job.dataset, uri)
        self.versions = get_versions(job.dataset, uri)
        super().__init__(uri or self.archive.uri)

    @classmethod
    def from_job(cls, job: DJ, dataset: Dataset) -> Self:
        """Create an operation bound to ``dataset``.

        Args:
            job: The job model instance
            dataset: The Dataset – provides the storage uri and stays bound
                as ``_dataset`` for operations that need the full handle
                (e.g. ``make`` / the index export).

        Returns:
            Configured operation instance
        """
        instance = cls(job, uri=dataset.uri)
        instance._dataset = dataset
        return instance

    def get_target(self) -> str:
        """Return the target tag. Override for dynamic values."""
        return self.target

    def get_dependencies(self) -> list[str]:
        """Return the dependencies. Override for dynamic values."""
        return self.dependencies

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        raise NotImplementedError

    def _run_local(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Core run logic – orchestration + handle()."""
        target = self.get_target()
        dependencies = self.get_dependencies()

        if not force:
            if target and dependencies:
                if self.tags.is_latest(target, dependencies):
                    self.job.log.info(
                        f"Already up-to-date: `{target}`, skipping ...",
                        target=target,
                        dependencies=dependencies,
                    )
                    self.job.stop()
                    return self.job

        # Execute: Store target tag and job result on successful context leave
        with self.jobs.run(self.job) as run, self.tags.touch(target) as now:
            self.job.log.info(
                f"Start `{target}` ...",
                target=target,
                dependencies=dependencies,
                started=now,
            )
            _ = self.handle(run, *args, force=force, **kwargs)
        self.log.info(
            f"Done `{target}`.",
            target=target,
            dependencies=dependencies,
            started=now,
            took=run.job.took,
            errors=run.job.errors,
        )
        return run.job

    @api_delegate("_api_run")
    def run(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Execute the handle function, force to run it regardless of freshness
        dependencies"""
        return self._run_local(force, *args, **kwargs)

    @require_api
    def _api_run(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Delegate run to remote api"""
        url = self._api.make_url("_api/operations")
        res = self._api.make_request(
            url,
            "POST",
            params={"force": force},
            json=self.job.model_dump(mode="json"),
        )
        return self.job.__class__(**res.json())

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.job.dataset})>"
