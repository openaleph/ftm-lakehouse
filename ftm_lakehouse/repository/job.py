"""JobRepository - job run storage using FileStore."""

import contextlib
from datetime import datetime
from typing import Generator, Generic

from anystore.logging import get_logger
from anystore.types import Uri

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.job import J, JobModel
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage.base import ModelStorage

log = get_logger(__name__)


class JobRun(Generic[J]):
    """Context manager for job run lifecycle."""

    def __init__(self, repo: "JobRepository", job: J) -> None:
        self.repo = repo
        self.job = job

    def start(self) -> None:
        """Mark job as started and save."""
        self.job.started = datetime.now()
        self.job.running = True
        self.repo.put(self.job)

    def save(self) -> None:
        """Save current job state."""
        self.job.touch()
        self.repo.put(self.job)

    def stop(self, exc: Exception | None = None) -> J:
        """Mark job as stopped and save."""
        self.job.stop(exc)
        self.repo.put(self.job)
        return self.job


class JobRepository(BaseRepository, Generic[J]):
    """
    Repository for job run storage.

    Uses ModelStorage to persist job run data as JSON files,
    organized by job type and run ID.

    Example:
        ```python
        repo = JobRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Store a job run
        repo.put(job)

        # Get latest run for a job type
        job = repo.latest(CrawlJob)

        # Run a job with lifecycle management
        with repo.run(job) as run:
            # Do work...
            run.save()  # Periodic save
        # Job automatically stopped when context exits
        ```
    """

    def __init__(self, dataset: str, uri: Uri, model: type[J]) -> None:
        super().__init__(dataset, uri)
        self.job_type = model.__name__
        self._store = ModelStorage(uri, model)._store

    def put(self, job: JobModel) -> None:
        """Store a job run."""
        self._store.put(path.job_run(self.job_type, job.run_id), job)

    def get(self, run_id: str) -> J:
        """Get a specific job run by type and run ID."""
        key = path.job_run(self.job_type, run_id)
        return self._store.get(key)

    def latest(self) -> J | None:
        """
        Get the latest run for the configured job type (self.model).

        Jobs are sorted by run ID (which contains timestamp),
        so the latest is the last in alphabetical order.
        """
        for key in sorted(
            self._store.iterate_keys(prefix=path.job_prefix(self.job_type)),
            reverse=True,
        ):
            return self._store.get(key)
        return None

    def iterate(self) -> Generator[J, None, None]:
        """Iterate all runs for the current job type."""
        yield from self._store.iterate_values(prefix=path.job_prefix(self.job_type))

    @contextlib.contextmanager
    def run(self, job: J) -> Generator[JobRun[J], None, None]:
        """
        Get a context manager for running a job.

        The job is automatically started on entry and stopped on exit.
        If an exception occurs, it's recorded in the job's exc field.
        """
        run = JobRun(self, job)
        try:
            run.start()
            yield run
        except Exception as e:
            run.stop(e)
            raise
        finally:
            if job.running:  # Only stop if not already stopped
                run.stop()

    def delete(self, job: J) -> None:
        """Delete a job run."""
        key = path.job_run(self.job_type, job.run_id)
        self._store.delete(key)
        self.log.warning("Deleted job run", job=job.name, run_id=job.run_id)
