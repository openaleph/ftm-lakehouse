from ftm_lakehouse.model import DatasetJobModel
from ftm_lakehouse.service import DatasetJobs, job_run


def test_jobs(tmp_path):
    class Job(DatasetJobModel):
        foo: str

    jobs = DatasetJobs("test_dataset", tmp_path / "jobs")

    job = Job.start(dataset="test_dataset", foo="bar")
    assert jobs.latest(Job) is None
    jobs.put(job)
    latest_job = jobs.latest(Job)
    assert latest_job is not None
    assert latest_job.run_id == job.run_id
    assert latest_job.foo == "bar"
    assert latest_job.running

    job.stop()
    jobs.put(job)
    latest_job = jobs.latest(Job)
    assert latest_job is not None
    assert not latest_job.running
    assert latest_job.took

    job = Job.make(foo="baz", dataset="test_dataset")
    with job_run(jobs, job) as run:
        assert run.job.running
        run.job.foo = "test"

    latest_job = jobs.latest(Job)
    assert latest_job is not None
    assert not latest_job.running
    assert latest_job.took
    assert latest_job.foo == "test"
