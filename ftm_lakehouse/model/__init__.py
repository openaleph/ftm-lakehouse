"""Data models for ftm_lakehouse."""

from ftm_lakehouse.model.catalog import CatalogModel, DatasetModel
from ftm_lakehouse.model.file import File, Files
from ftm_lakehouse.model.job import DEFAULT_USER, DatasetJobModel, JobModel
from ftm_lakehouse.model.mapping import (
    DatasetMapping,
    EntityMapping,
    Mapping,
    PropertyMapping,
    mapping_origin,
)

__all__ = [
    # Catalog
    "CatalogModel",
    "DatasetModel",
    # File
    "File",
    "Files",
    # Job
    "DEFAULT_USER",
    "DatasetJobModel",
    "JobModel",
    # Mapping
    "DatasetMapping",
    "EntityMapping",
    "Mapping",
    "PropertyMapping",
    "mapping_origin",
]
