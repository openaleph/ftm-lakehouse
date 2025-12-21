from datetime import timedelta

from ftm_lakehouse.model.job import DatasetJobModel, JobModel


def test_job_model_make():
    """Test creating a job with make()"""
    job = JobModel.make()
    assert job.run_id is not None
    assert job.running is False
    assert job.pending == 0
    assert job.done == 0
    assert job.errors == 0
    assert job.name == "JobModel"


def test_job_model_start():
    """Test starting a job"""
    job = JobModel.start()
    assert job.run_id is not None
    assert job.running is True
    assert job.started is not None
    assert job.last_updated is not None


def test_job_model_stop():
    """Test stopping a job"""
    job = JobModel.start()
    job.stop()
    assert job.running is False
    assert job.stopped is not None
    assert isinstance(job.took, timedelta)


def test_job_model_touch():
    """Test updating last_updated"""
    job = JobModel.make()
    assert job.last_updated is None
    job.touch()
    assert job.last_updated is not None
    ts = job.last_updated
    job.touch()
    assert job.last_updated > ts


def test_dataset_job_model():
    """Test DatasetJobModel like CrawlJob"""
    job = DatasetJobModel.make(dataset="test_dataset")
    assert job.dataset == "test_dataset"
    assert job.name == "DatasetJobModel"
    assert job.log is not None
