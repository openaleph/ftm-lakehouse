"""Tests for JobRepository - job run storage."""

from ftm_lakehouse.model.job import JobModel
from ftm_lakehouse.repository.job import JobRepository

DATASET = "test"


class SampleJob(JobModel):
    """Simple job model for testing."""

    message: str = ""


class OtherJob(SampleJob):
    pass


def test_repository_job_put_path(tmp_path):
    """Test put() stores job at hardcoded path."""
    repo = JobRepository(dataset=DATASET, uri=tmp_path, model=SampleJob)
    job = SampleJob.make(run_id="run-123", message="hello")

    repo.put(job)

    # Job should be stored at jobs/runs/{JobTypeName}/{run_id}.json
    expected_path = tmp_path / "jobs" / "runs" / "SampleJob" / "run-123.json"
    assert expected_path.exists()


def test_repository_job_latest(tmp_path):
    """Test latest() returns most recent job by run_id."""
    repo = JobRepository(dataset=DATASET, uri=tmp_path, model=SampleJob)

    # Create jobs with sortable run_ids
    repo.put(SampleJob.make(run_id="2025-01-01", message="first"))
    repo.put(SampleJob.make(run_id="2025-01-03", message="third"))
    repo.put(SampleJob.make(run_id="2025-01-02", message="second"))

    latest = repo.latest()
    assert latest is not None
    assert latest.run_id == "2025-01-03"
    assert latest.message == "third"


def test_repository_job_iterate(tmp_path):
    """Test iterate() yields all jobs for type."""
    repo = JobRepository(dataset=DATASET, uri=tmp_path, model=SampleJob)
    other_repo = JobRepository(dataset=DATASET, uri=tmp_path, model=OtherJob)

    repo.put(SampleJob.make(run_id="run-a", message="a"))
    repo.put(SampleJob.make(run_id="run-b", message="b"))
    other_repo.put(OtherJob.make(run_id="run-c", message="c"))

    jobs = list(repo.iterate())
    assert len(jobs) == 2  # only SampleJob in this repo
    assert len(list(other_repo.iterate())) == 1  # OtherJob
    messages = {j.message for j in jobs}
    assert messages == {"a", "b"}


def test_repository_job_run_context_manager(tmp_path):
    """Test run() context manager lifecycle."""
    repo = JobRepository(dataset=DATASET, uri=tmp_path, model=SampleJob)
    job = SampleJob.make(run_id="ctx-run", message="context test")

    with repo.run(job) as run:
        # Job should be started and running
        assert run.job.running is True
        assert run.job.started is not None

        # Can save intermediate state
        run.job.done = 5
        run.save()

        # Verify saved
        saved = repo.get("ctx-run")
        assert saved.done == 5
        run.job.done = 6

    # After context, job should be stopped
    final = repo.get("ctx-run")
    assert final.running is False
    assert final.stopped is not None
    assert final.done == 6


def test_repository_job_run_context_manager_exception(tmp_path):
    """Test run() context manager records exception."""
    repo = JobRepository(dataset=DATASET, uri=tmp_path, model=SampleJob)
    job = SampleJob.make(run_id="exc-run", message="exception test")

    try:
        with repo.run(job) as run:
            run.job.done = 3
            raise ValueError("Test error")
    except ValueError:
        pass

    # Job should have exception recorded
    final = repo.get("exc-run")
    assert final.running is False
    assert final.exc == "Test error"
    assert final.done == 3
