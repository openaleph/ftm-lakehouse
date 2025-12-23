"""
Factory functions for the repositories that fall back to the default configured
settings. Useful for override runtime config uri during testing as well as for
public convenience.
"""

from anystore.functools import weakref_cache as cache
from anystore.types import Uri

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.repository.archive import ArchiveRepository
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.job import J, JobRepository
from ftm_lakehouse.repository.mapping import MappingRepository
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.versions import VersionStore


@cache
def get_archive(dataset: str, uri: Uri | None = None) -> ArchiveRepository:
    settings = Settings()
    uri = uri or f"{settings.uri}/{dataset}"
    return ArchiveRepository(dataset, uri)


@cache
def get_entities(
    dataset: str, uri: Uri | None = None, journal_uri: str | None = None
) -> EntityRepository:
    settings = Settings()
    uri = uri or f"{settings.uri}/{dataset}"
    return EntityRepository(dataset, uri, journal_uri)


@cache
def get_mappings(dataset: str, uri: Uri | None = None) -> MappingRepository:
    settings = Settings()
    uri = uri or f"{settings.uri}/{dataset}"
    return MappingRepository(dataset, uri)


@cache
def get_jobs(dataset: str, model: type[J], uri: Uri | None = None) -> JobRepository[J]:
    settings = Settings()
    uri = uri or f"{settings.uri}/{dataset}"
    return JobRepository(dataset, uri, model)


@cache
def get_versions(dataset: str, uri: Uri | None = None) -> VersionStore:
    settings = Settings
    uri = uri or f"{settings.uri}/{dataset}"
    return VersionStore(uri)


@cache
def get_tags(
    dataset: str, uri: Uri | None = None, tenant: str | None = None
) -> TagStore:
    settings = Settings()
    uri = uri or f"{settings.uri}/{dataset}"
    return TagStore(uri, tenant)
