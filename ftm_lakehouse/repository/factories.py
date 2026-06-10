"""
Factory functions for the repositories that fall back to the default configured
settings. These are the single instantiation path for repositories: both
module-level callers (``get_entities("my_dataset")``) and ``Dataset`` method
accessors (``dataset.get_entities()``) resolve through the same cache.

All factories are LRU-cached at :data:`LRU_MAX` entries: generous enough to
cover any realistic multi-tenant dataset count in a single process, but
bounded so an attacker that probes many distinct dataset names cannot
permanently retain a repository (and its SQLAlchemy engine / DuckDB
connection) per probe.

The cache key is the canonical dataset URI from :func:`dataset_uri` – the
same storage location always resolves to the same instance, whether
addressed by name only (settings-derived) or by an explicit uri (str or
``Path``, with or without scheme).
"""

from functools import lru_cache

from anystore.types import Uri
from anystore.util import ensure_uri, join_uri

from ftm_lakehouse.core.api import ensure_api_uri
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.documents import DocumentRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.job import J, JobRepository
from ftm_lakehouse.repository.mapping import MappingRepository
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore

LRU_MAX = 1024
"""Maximum number of distinct dataset keys retained per factory."""


def dataset_uri(dataset: str, uri: Uri | None = None) -> str:
    """Canonical URI for a dataset – same location, same string, same cache key.

    ``None`` derives ``{LAKEHOUSE_URI}/{dataset}`` exactly like
    :func:`ftm_lakehouse.lake.get_lakehouse` does for the catalog; explicit
    values (str or ``Path``) are normalized via ``ensure_uri``.
    """
    if uri is not None:
        return str(ensure_uri(uri))
    settings = Settings()
    return str(join_uri(ensure_uri(settings.uri), dataset))


def get_archive(dataset: str, uri: Uri | None = None) -> ArchiveRepository:
    """
    Get the archive repository for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        ArchiveRepository instance (cached)
    """
    return _get_archive(dataset, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_archive(dataset: str, uri: str) -> ArchiveRepository:
    return ArchiveRepository(dataset, uri)


def get_entities(dataset: str, uri: Uri | None = None) -> EntityRepository:
    """
    Get the entity repository for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        EntityRepository instance (cached)
    """
    return _get_entities(dataset, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_entities(dataset: str, uri: str) -> EntityRepository:
    return EntityRepository(dataset, uri)


def get_documents(dataset: str, uri: Uri | None = None) -> DocumentRepository:
    """
    Get the document repository for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        DocumentRepository instance (cached)
    """
    return _get_documents(dataset, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_documents(dataset: str, uri: str) -> DocumentRepository:
    return DocumentRepository(dataset, uri)


def get_mappings(dataset: str, uri: Uri | None = None) -> MappingRepository:
    """
    Get the mappings repository for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        MappingRepository instance (cached)
    """
    return _get_mappings(dataset, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_mappings(dataset: str, uri: str) -> MappingRepository:
    return MappingRepository(dataset, uri)


def get_jobs(dataset: str, model: type[J], uri: Uri | None = None) -> JobRepository[J]:
    """
    Get the job repository for a dataset.

    Args:
        dataset: Dataset name
        model: Job model class
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        JobRepository instance (cached)
    """
    return _get_jobs(dataset, model, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_jobs(dataset: str, model: type[J], uri: str) -> JobRepository[J]:
    return JobRepository(dataset, uri, model)


def get_versions(dataset: str, uri: Uri | None = None) -> VersionStore:
    """
    Get the version store for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})

    Returns:
        VersionStore instance (cached)
    """
    return _get_versions(dataset, dataset_uri(dataset, uri))


@lru_cache(maxsize=LRU_MAX)
def _get_versions(dataset: str, uri: str) -> VersionStore:
    return VersionStore(ensure_api_uri(uri))


def get_tags(
    dataset: str, uri: Uri | None = None, tenant: str | None = None
) -> TagStore:
    """
    Get the tag store for a dataset.

    Args:
        dataset: Dataset name
        uri: Dataset URI override (default: {LAKEHOUSE_URI}/{dataset})
        tenant: Tag tenant/namespace

    Returns:
        TagStore instance (cached)
    """
    return _get_tags(dataset, dataset_uri(dataset, uri), tenant)


@lru_cache(maxsize=LRU_MAX)
def _get_tags(dataset: str, uri: str, tenant: str | None = None) -> TagStore:
    return TagStore(ensure_api_uri(uri), tenant)


def clear_caches() -> None:
    """Clear all factory caches – test isolation between runs."""
    _get_archive.cache_clear()
    _get_entities.cache_clear()
    _get_documents.cache_clear()
    _get_mappings.cache_clear()
    _get_jobs.cache_clear()
    _get_versions.cache_clear()
    _get_tags.cache_clear()
