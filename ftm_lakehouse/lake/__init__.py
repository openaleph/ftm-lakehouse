"""
Lake module - high-level orchestration layer.

This module provides the main entry points for working with the FTM Lakehouse:

- Lakehouse: Top-level catalog managing multiple datasets
- DatasetLakehouse: Individual dataset with all operations
- DatasetEntities: Entity read/write/query operations
- DatasetMappings: CSV mapping transformations

Factory functions:

- get_lakehouse(): Get the configured lakehouse instance
- get_dataset(): Get a dataset by name
- get_archive(): Get the file archive for a dataset
- get_entities(): Get the entities interface for a dataset
- get_mappings(): Get the mappings interface for a dataset
"""

from ftm_lakehouse.lake.entities import DatasetEntities
from ftm_lakehouse.lake.lakehouse import (
    DM,
    DatasetLakehouse,
    Lakehouse,
    get_archive,
    get_dataset,
    get_entities,
    get_lakehouse,
    get_mappings,
)
from ftm_lakehouse.lake.mappings import DatasetMappings

__all__ = [
    "DM",
    "DatasetEntities",
    "DatasetLakehouse",
    "DatasetMappings",
    "Lakehouse",
    "get_archive",
    "get_dataset",
    "get_entities",
    "get_lakehouse",
    "get_mappings",
]
