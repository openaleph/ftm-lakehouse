"""Layer 3: Domain-specific repository combinations.

Each repository combines multiple stores for a single domain concept.
No cross-domain awareness.
"""

from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.documents import DocumentRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_documents,
    get_entities,
    get_jobs,
    get_mappings,
)
from ftm_lakehouse.repository.job import JobRepository
from ftm_lakehouse.repository.mapping import MappingRepository

__all__ = [
    "ArchiveRepository",
    "DocumentRepository",
    "EntityRepository",
    "JobRepository",
    "MappingRepository",
    "get_archive",
    "get_documents",
    "get_entities",
    "get_jobs",
    "get_mappings",
]
