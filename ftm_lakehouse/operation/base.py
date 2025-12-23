from typing import Generic

from anystore.types import Uri

from ftm_lakehouse.core.freshness import is_latest
from ftm_lakehouse.model.job import DJ
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_entities,
    get_jobs,
    get_tags,
)
from ftm_lakehouse.repository.job import JobRepository, JobRun
from ftm_lakehouse.storage.tags import TagStore


class DatasetJobOperation(Generic[DJ]):
    """
    A (long-running) operation for a specific dataset that updates tags and
    checks dependencies for freshness to be able to skip this operation. The job
    result is stored after successful run.
    """

    target: str  # tag that gets touched after successful run
    dependencies: list[str] = []  # dependencies for freshness check

    def __init__(
        self,
        job: DJ,
        archive: ArchiveRepository | None = None,
        entities: EntityRepository | None = None,
        tags: TagStore | None = None,
        jobs: JobRepository | None = None,
        lake_uri: Uri | None = None,
    ) -> None:
        self.dataset = job.dataset
        self.job = job
        self.log = job.log
        self.archive = archive or get_archive(job.dataset, lake_uri)
        self.entities = entities or get_entities(job.dataset, lake_uri)
        self.jobs = jobs or get_jobs(job.dataset, job.__class__, lake_uri)
        self.tags = tags or get_tags(job.dataset, lake_uri)

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        raise NotImplementedError

    def run(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Execute the handle function, force to run it regardless of freshness
        dependencies"""
        if not force:
            if self.target and self.dependencies:
                if is_latest(self.tags, self.target, self.dependencies):
                    self.job.log.info(
                        f"Already up-to-date: `{self.target}`, skipping ...",
                        target=self.target,
                        dependencies=self.dependencies,
                    )
                    self.job.stop()
                    return self.job

        # Execute: Store target tag and job result on successful context leave
        with self.jobs.run(self.job) as run, self.tags.touch(self.target) as now:
            self.job.log.info(
                f"Start `{self.target}` ...",
                target=self.target,
                dependencies=self.dependencies,
                started=now,
            )
            _ = self.handle(run, *args, **kwargs)
        self.log.info(
            f"Done `{self.target}`.",
            target=self.target,
            dependencies=self.dependencies,
            started=now,
            took=run.job.took,
            errors=run.job.errors,
        )
        result = self.jobs.latest()
        if result is not None:
            return result
        raise RuntimeError("Result is `None`")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
