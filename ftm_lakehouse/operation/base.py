from __future__ import annotations

from functools import cached_property
from typing import Generic

from anystore.types import Uri

from ftm_lakehouse.model.job import DJ
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.artifacts import ArtifactsRepository
from ftm_lakehouse.repository.base import DatasetHandle, dataset_uri
from ftm_lakehouse.repository.documents import DocumentRepository
from ftm_lakehouse.repository.entities.main import EntityRepository
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_artifacts,
    get_documents,
    get_entities,
    get_jobs,
)
from ftm_lakehouse.repository.job import JobRepository, JobRun


class DatasetJobOperation(DatasetHandle, Generic[DJ]):
    """A (long-running) dataset operation, skipped while its target tag is
    newer than its dependencies; the job result is stored after a successful run.

    Repositories come from the LRU-cached factories. Subclasses set `target` and
    `dependencies`, or override `get_target()` / `get_dependencies()`.
    """

    target: str = ""  # tag that gets touched after successful run
    dependencies: list[str] = []  # dependencies for freshness check

    def __init__(self, job: DJ, uri: Uri | None = None) -> None:
        super().__init__(job.dataset, dataset_uri(job.dataset, uri))
        self.job = job
        self.log = job.log

    @cached_property
    def archive(self) -> ArchiveRepository:
        return get_archive(self.dataset, self.uri)

    @cached_property
    def artifacts(self) -> ArtifactsRepository:
        return get_artifacts(self.dataset, self.uri)

    @cached_property
    def entities(self) -> EntityRepository:
        return get_entities(self.dataset, self.uri)

    @cached_property
    def documents(self) -> DocumentRepository:
        return get_documents(self.dataset, self.uri)

    @cached_property
    def jobs(self) -> JobRepository:
        return get_jobs(self.dataset, self.job.__class__, self.uri)

    def get_target(self) -> str:
        """Return the target tag. Override for dynamic values."""
        return self.target

    def get_dependencies(self) -> list[str]:
        """Return the dependencies. Override for dynamic values."""
        return self.dependencies

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        raise NotImplementedError

    def prepare(self) -> None:
        """Bring the dataset into the state `handle` reads from – no-op by default.

        Runs before the freshness check and outside the target's window:
        `Tags.touch` stamps the target with its *entry* time, so a dependency
        moved inside the window would mark the result stale at once.
        [`ExportOperation`][ftm_lakehouse.operation.export.ExportOperation]
        drains the journal here.
        """

    def is_fresh(self) -> bool:
        """Whether the target tag is newer than every dependency – nothing to
        do. Override where freshness is not a pair of timestamps."""
        target = self.get_target()
        dependencies = self.get_dependencies()
        if not (target and dependencies):
            return False
        return self._tags.is_latest(target, dependencies)

    def _run_local(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Core run logic – prepare() + orchestration + handle()."""
        target = self.get_target()
        dependencies = self.get_dependencies()

        self.prepare()

        if not force:
            if self.is_fresh():
                self.job.log.info(
                    f"Already up-to-date: `{target}`, skipping ...",
                    target=target,
                    dependencies=dependencies,
                )
                self.job.stop()
                return self.job

        # target tag and job result are stored on a clean exit
        with self.jobs.run(self.job) as run, self._tags.touch(target) as now:
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

    def run(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Run the operation unless fresh (``force`` runs it anyway); in api mode
        the job is delegated to the server's operations endpoint."""
        if self._is_api:
            return self._api_run(force, *args, **kwargs)
        return self._run_local(force, *args, **kwargs)

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
