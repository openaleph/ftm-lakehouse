"""
Factory functions for the repositories that fall back to the default configured
settings. These are the single instantiation path for repositories – every
caller, from the module-level ``get_entities("my_dataset")`` convenience to
operations and the API server, resolves through the same cache.

All factories share one LRU cache of :data:`LRU_MAX` entries, keyed on the
builder callable plus the canonical dataset URI from :func:`dataset_uri` –
the same storage location always resolves to the same instance, whether
addressed by name only (settings-derived) or by an explicit uri (str or
``Path``, with or without scheme).
"""

from functools import lru_cache
from typing import Any, Callable, cast

from anystore.types import Uri

from ftm_lakehouse.core.api import ensure_api_uri, get_api
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.base import dataset_uri
from ftm_lakehouse.repository.documents import DocumentRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.entities.api import ApiEntityRepository
from ftm_lakehouse.repository.job import J, JobRepository
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore

__all__ = [
    "dataset_uri",
    "get_archive",
    "get_entities",
    "get_documents",
    "get_jobs",
    "get_versions",
    "get_tags",
    "clear_caches",
]

LRU_MAX = 4096
"""Maximum number of entries retained across all factory kinds (one shared
cache): generous enough to cover any realistic multi-tenant dataset count in
a single process, but bounded so an attacker that probes many distinct
dataset names cannot permanently retain a repository (and its SQLAlchemy
engine / DuckDB connection) per probe."""


@lru_cache(maxsize=LRU_MAX)
def _resolve(builder: Callable[..., Any], *args: Any) -> Any:
    """One shared LRU over ``(builder, canonical args)`` – the builder
    callable is part of the key, so kinds never collide."""
    return builder(*args)


def _build_entities(dataset: str, uri: str) -> EntityRepository:
    # Construction-time api pick, mirroring `get_journal`'s Sql-vs-Api choice
    # - but keyed on the dataset uri, not global settings.
    if get_api(uri) is not None:
        return ApiEntityRepository(dataset, uri)
    return EntityRepository(dataset, uri)


def _build_jobs(dataset: str, uri: str, model: type[J]) -> JobRepository[J]:
    return JobRepository(dataset, uri, model)


def _build_versions(dataset: str, uri: str) -> VersionStore:
    return VersionStore(ensure_api_uri(uri))


def _build_tags(dataset: str, uri: str, tenant: str | None) -> TagStore:
    return TagStore(ensure_api_uri(uri), tenant)


def get_archive(dataset: str, uri: Uri | None = None) -> ArchiveRepository:
    """Get the archive repository for a dataset (cached)."""
    return cast(
        ArchiveRepository,
        _resolve(ArchiveRepository, dataset, dataset_uri(dataset, uri)),
    )


def get_entities(dataset: str, uri: Uri | None = None) -> EntityRepository:
    """Get the entity repository for a dataset (cached; the api-mode
    subclass for http uris)."""
    return cast(
        EntityRepository, _resolve(_build_entities, dataset, dataset_uri(dataset, uri))
    )


def get_documents(dataset: str, uri: Uri | None = None) -> DocumentRepository:
    """Get the document repository for a dataset (cached)."""
    return cast(
        DocumentRepository,
        _resolve(DocumentRepository, dataset, dataset_uri(dataset, uri)),
    )


def get_jobs(dataset: str, model: type[J], uri: Uri | None = None) -> JobRepository[J]:
    """Get the job repository for a dataset and job model class (cached)."""
    return cast(
        JobRepository[J],
        _resolve(_build_jobs, dataset, dataset_uri(dataset, uri), model),
    )


def get_versions(dataset: str, uri: Uri | None = None) -> VersionStore:
    """Get the version store for a dataset (cached)."""
    return cast(
        VersionStore, _resolve(_build_versions, dataset, dataset_uri(dataset, uri))
    )


def get_tags(
    dataset: str, uri: Uri | None = None, tenant: str | None = None
) -> TagStore:
    """Get the tag store for a dataset (cached)."""
    return cast(
        TagStore, _resolve(_build_tags, dataset, dataset_uri(dataset, uri), tenant)
    )


def clear_caches() -> None:
    """Clear all factory caches – test isolation and config-write invalidation.

    Called by :func:`ftm_lakehouse.catalog.update_dataset` after a
    ``config.yml`` write so newly fetched repositories see the fresh model
    snapshot; repositories held across the write keep their old snapshot.
    """
    _resolve.cache_clear()
