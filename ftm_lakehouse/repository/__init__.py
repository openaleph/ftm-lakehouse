"""Layer 3: Domain-specific repository combinations.

Each repository combines multiple stores for a single domain concept.
No cross-domain awareness.
"""

from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.artifacts import ArtifactsRepository
from ftm_lakehouse.repository.documents import DocumentRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_artifacts,
    get_documents,
    get_entities,
    get_jobs,
)
from ftm_lakehouse.repository.job import JobRepository

__all__ = [
    "ArchiveRepository",
    "ArtifactsRepository",
    "DocumentRepository",
    "EntityRepository",
    "JobRepository",
    "get_archive",
    "get_artifacts",
    "get_documents",
    "get_entities",
    "get_jobs",
]
