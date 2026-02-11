"""Data models for ftm_lakehouse."""

from ftm_lakehouse.model.crud import Crud, CrudAction, CrudResource
from ftm_lakehouse.model.dataset import DM, CatalogModel, DatasetModel
from ftm_lakehouse.model.file import File, Files
from ftm_lakehouse.model.job import DatasetJobModel, JobModel
from ftm_lakehouse.model.mapping import DatasetMapping, mapping_origin

__all__ = [
    # Catalog
    "CatalogModel",
    "DatasetModel",
    "DM",
    # Crud
    "Crud",
    "CrudAction",
    "CrudResource",
    # File
    "File",
    "Files",
    # Job
    "DatasetJobModel",
    "JobModel",
    # Mapping
    "DatasetMapping",
    "mapping_origin",
]
