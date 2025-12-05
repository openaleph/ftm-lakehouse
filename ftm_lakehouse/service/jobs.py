import contextlib
from datetime import datetime
from typing import ContextManager, Generator, Generic, Type, TypeVar

from ftm_lakehouse.conventions import path
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.model import JobModel

Job = TypeVar("Job", bound=JobModel)


class JobRun(Generic[Job]):
    def __init__(self, lake: LakeMixin, job: Job) -> None:
        self.jobs = DatasetJobs(lake.name, lake.uri)
        self.job = job

    def start(self) -> None:
        self.job.started = datetime.now()
        self.job.running = True
        self.jobs.put(self.job)

    def save(self) -> None:
        self.job.touch()
        self.jobs.put(self.job)

    def stop(self, exc: Exception | None = None) -> Job:
        self.job.stop(exc)
        self.jobs.put(self.job)
        return self.job


@contextlib.contextmanager
def job_run(lake: LakeMixin, job: JobModel) -> Generator[JobRun, None, None]:
    run = JobRun(lake, job)
    try:
        run.start()
        yield run
    except Exception as e:
        run.stop(e)
    finally:
        run.stop()


class DatasetJobs(LakeMixin):
    """
    Interface to store and retrieve job runs result data for a lake dataset

    Example:
        Get the latest crawl run:
        ```python
        from ftm_lakehouse import get_dataset
        from ftm_lakehouse.logic.crawl import CrawlJob

        dataset = get_dataset("my_dataset")
        print(dataset.jobs.latest(CrawlJob))
        ```
    """

    def put(self, job: JobModel) -> None:
        """Store status for a job"""
        key = f"{path.JOB_RUNS}/{job.name}/{job.run_id}.json"
        self.storage.put(key, job, model=job.__class__)

    def latest(self, job_type: Type[Job]) -> Job | None:
        """Get the latest run status for a job type"""
        prefix = f"{path.JOB_RUNS}/{job_type.get_name()}"
        for key in sorted(self.storage.iterate_keys(prefix=prefix), reverse=True):
            return self.storage.get(key, model=job_type)
        return None

    def run(self, job: JobModel) -> ContextManager[JobRun]:
        return job_run(self, job)
