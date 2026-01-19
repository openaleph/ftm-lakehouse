"""Dataset class for single-dataset management."""

from functools import cached_property
from typing import Any, Generic, TypeVar

from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri

from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model import DatasetModel
from ftm_lakehouse.repository import (
    ArchiveRepository,
    EntityRepository,
    JobRepository,
    MappingRepository,
)
from ftm_lakehouse.storage import TagStore
from ftm_lakehouse.storage.versions import VersionStore

log = get_logger(__name__)

DM = TypeVar("DM", bound=DatasetModel)


class Dataset(Generic[DM]):
    """
    A single dataset within the lakehouse.

    Provides access to repositories for domain operations:

    - archive: File storage (ArchiveRepository)
    - entities: Entity/statement operations (EntityRepository)
    - mappings: Mapping configurations (MappingRepository)
    - jobs: Job tracking (JobRepository)

    Example:
        ```python
        from ftm_lakehouse import get_dataset

        dataset = get_dataset("my_dataset")
        dataset.ensure()

        # Add entities
        dataset.entities.add(entity, origin="import")

        # Archive files
        dataset.archive.put(uri)

        # Update config
        dataset.update_model(title="New Title")
        ```
    """

    def __init__(
        self,
        name: str,
        uri: Uri,
        model_class: type[DM] = DatasetModel,
    ) -> None:
        self.name = name
        self.uri = uri
        self._model_class = model_class
        self._settings = Settings()
        self._log = log.bind(dataset=name, uri=uri)

    def __repr__(self) -> str:
        return f"Dataset({self.name!r})"

    # -------------------------------------------------------------------------
    # Storage primitives
    # -------------------------------------------------------------------------

    @cached_property
    def _store(self):
        """Raw storage access."""
        return get_store(uri=self.uri, serialization_mode="raw")

    @cached_property
    def _tags(self) -> TagStore:
        """Tag store for freshness tracking."""
        return TagStore(self.uri)

    @cached_property
    def _versions(self) -> VersionStore:
        """Version store for snapshots."""
        return VersionStore(self.uri)

    # -------------------------------------------------------------------------
    # Model access (config.yml via VersionStore)
    # -------------------------------------------------------------------------

    def _load_model(self, **data: Any) -> DM:
        """Load dataset model from config.yml."""
        data["name"] = self.name
        return self._model_class(**load_config(self._store, **data))

    @property
    def model(self) -> DM:
        """Load and return the dataset model from config.yml."""
        return self._load_model()

    def update_model(self, **data: Any) -> DM:
        """
        Update config.yml with new data.

        Uses VersionStore to create versioned snapshots.

        Args:
            **data: Fields to update in the model

        Returns:
            Updated model
        """
        model = self._load_model(**data)
        self._versions.make(path.CONFIG, model)
        self._log.info("Updated dataset config", **data)
        return model

    # -------------------------------------------------------------------------
    # Repositories (cached, initialized on first access)
    # -------------------------------------------------------------------------

    @cached_property
    def archive(self) -> ArchiveRepository:
        """File archive operations."""
        return ArchiveRepository(self.name, self.uri)

    @cached_property
    def entities(self) -> EntityRepository:
        """Entity/statement operations."""
        return EntityRepository(self.name, self.uri, self._settings.journal_uri)

    @cached_property
    def mappings(self) -> MappingRepository:
        """Mapping configuration storage."""
        return MappingRepository(self.name, self.uri)

    @cached_property
    def jobs(self) -> JobRepository:
        """Job tracking."""
        from ftm_lakehouse.model import DatasetJobModel

        return JobRepository(self.name, self.uri, DatasetJobModel)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def exists(self) -> bool:
        """Check if dataset exists (has config.yml)."""
        return self._store.exists(path.CONFIG)

    def ensure(self) -> None:
        """Ensure dataset exists, create config.yml if needed."""
        if not self.exists():
            self.update_model()
            self._log.info("Created dataset")
