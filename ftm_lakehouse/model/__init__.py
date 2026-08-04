"""Data models for ftm_lakehouse."""

from ftm_lakehouse.model.dataset import (
    DatasetModel,
    get_model_class,
    set_model_class,
)
from ftm_lakehouse.model.file import File, Files
from ftm_lakehouse.model.job import DatasetJobModel, JobModel
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    StatementRow,
    StatementRows,
)

__all__ = [
    # Dataset
    "DatasetModel",
    "get_model_class",
    "set_model_class",
    # File
    "File",
    "Files",
    # Job
    "DatasetJobModel",
    "JobModel",
    # Statement schema
    "SHARDED_SCHEMA",
    "StatementRow",
    "StatementRows",
    "TABLE",
]
