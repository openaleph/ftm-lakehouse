"""Data models for ftm_lakehouse."""

from ftm_lakehouse.model.dataset import DM, CatalogModel, DatasetModel
from ftm_lakehouse.model.file import File, Files
from ftm_lakehouse.model.job import DatasetJobModel, JobModel
from ftm_lakehouse.model.mapping import DatasetMapping, mapping_origin
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    StatementRow,
    StatementRows,
)

__all__ = [
    # Catalog
    "CatalogModel",
    "DatasetModel",
    "DM",
    # File
    "File",
    "Files",
    # Job
    "DatasetJobModel",
    "JobModel",
    # Mapping
    "DatasetMapping",
    "mapping_origin",
    # Statement schema
    "SHARDED_SCHEMA",
    "StatementRow",
    "StatementRows",
    "TABLE",
]
