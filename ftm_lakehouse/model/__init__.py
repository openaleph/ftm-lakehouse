"""Data models for ftm_lakehouse."""

from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.model.job import DatasetJobModel, JobModel

__all__ = [
    # Dataset
    "DatasetModel",
    # Job
    "DatasetJobModel",
    "JobModel",
]
