from typing import Generic

from anystore.types import Uri

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.job import DJ
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_entities,
    get_jobs,
    get_tags,
    get_versions,
)
from ftm_lakehouse.repository.job import JobRepository, JobRun
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


class DatasetJobOperation(Generic[DJ]):
    """
    A (long-running) operation for a specific dataset that updates tags and
    checks dependencies for freshness to be able to skip this operation. The job
    result is stored after successful run.

    Subclasses can either set class attributes `target` and `dependencies`,
    or override `get_target()` and `get_dependencies()` for dynamic values.
    """

    target: str = ""  # tag that gets touched after successful run
    dependencies: list[str] = []  # dependencies for freshness check

    def __init__(
        self,
        job: DJ,
        archive: ArchiveRepository | None = None,
        entities: EntityRepository | None = None,
        jobs: JobRepository | None = None,
        tags: TagStore | None = None,
        versions: VersionStore | None = None,
        lake_uri: Uri | None = None,
    ) -> None:
        settings = Settings()
        self.uri = lake_uri or settings.uri
        self.dataset = job.dataset
        self.job = job
        self.log = job.log
        self.archive = archive or get_archive(job.dataset, self.uri)
        self.entities = entities or get_entities(job.dataset, self.uri)
        self.jobs = jobs or get_jobs(job.dataset, job.__class__, self.uri)
        self.tags = tags or get_tags(job.dataset, self.uri)
        self.versions = versions or get_versions(job.dataset, self.uri)

    def get_target(self) -> str:
        """Return the target tag. Override for dynamic values."""
        return self.target

    def get_dependencies(self) -> list[str]:
        """Return the dependencies. Override for dynamic values."""
        return self.dependencies

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        raise NotImplementedError

    def run(self, force: bool | None = False, *args, **kwargs) -> DJ:
        """Execute the handle function, force to run it regardless of freshness
        dependencies"""
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
        result = self.jobs.latest()
        if result is not None:
            return result
        raise RuntimeError("Result is `None`")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.dataset})>"
