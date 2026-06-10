"""Dataset class for single-dataset management."""

from typing import Any, Generic

from anystore.logging import get_logger
from anystore.store import Store, get_store
from anystore.types import Uri
from anystore.util import mask_uri

from ftm_lakehouse.core.api import ensure_api_uri
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs import ensure_zfs_dataset
from ftm_lakehouse.model import DM, DatasetModel
from ftm_lakehouse.repository import (
    ArchiveRepository,
    DocumentRepository,
    EntityRepository,
    MappingRepository,
)
from ftm_lakehouse.repository.factories import (
    get_archive,
    get_documents,
    get_entities,
    get_mappings,
    get_versions,
)
from ftm_lakehouse.storage.versions import VersionStore

log = get_logger(__name__)


class Dataset(Generic[DM]):
    """
    A single dataset within the lakehouse – a stateless handle around the
    dataset's name, storage uri and configuration.

    Repository access goes through the LRU-cached factories, so every path
    addressing the same dataset – this handle, the module-level
    ``get_entities("name")`` convenience functions, and operations – shares
    one repository instance:

    - `get_archive()`: File storage (ArchiveRepository)
    - `get_entities()`: Entity/statement operations (EntityRepository)
    - `get_documents()`: Document metadata (DocumentRepository)
    - `get_mappings()`: Mapping configurations (MappingRepository)

    Example:
        ```python
        from ftm_lakehouse import get_dataset

        dataset = get_dataset("my_dataset")
        dataset.ensure()

        # Add entities
        dataset.get_entities().add(entity, origin="import")

        # Archive files
        dataset.get_archive().store(uri)

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
        self._log = log.bind(dataset=name, uri=mask_uri(uri))

        if self._store.is_local and self._settings.on_zfs:
            if self._settings.zfs_pool is None:
                raise RuntimeError("Configure LAKEHOUSE_ZFS_POOL for zfs integration!")
            ensure_zfs_dataset(self._settings.zfs_pool, self.name)

    def __repr__(self) -> str:
        return f"Dataset({self.name!r})"

    # -------------------------------------------------------------------------
    # Storage primitives
    # -------------------------------------------------------------------------

    @property
    def _store(self) -> Store:
        """Raw storage access."""
        return get_store(uri=ensure_api_uri(self.uri), serialization_mode="raw")

    @property
    def _versions(self) -> VersionStore:
        """Version store for snapshots."""
        return get_versions(self.name, self.uri)

    # -------------------------------------------------------------------------
    # Model access (config.yml via VersionStore)
    # -------------------------------------------------------------------------

    def _load_model(self, **data: Any) -> DM:
        """Load dataset model from config.yml."""
        data["name"] = self.name
        data.pop("storage", None)
        return self._model_class(**load_config(self._store, **data))

    @property
    def model(self) -> DM:
        """Load and return the dataset model from config.yml."""
        return self._load_model()

    @property
    def index(self) -> DM:
        """Load and return the generated index.json (or fall back to config.yml)"""
        index = self._versions.get(path.INDEX, model=self._model_class)
        if index:
            return index
        return self.model

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
        self._log.info("Updated dataset config")
        return model

    # -------------------------------------------------------------------------
    # Repositories (resolved through the LRU-cached factories)
    # -------------------------------------------------------------------------

    def get_archive(self) -> ArchiveRepository:
        """File archive operations."""
        return get_archive(self.name, self.uri)

    def get_entities(self) -> EntityRepository:
        """Entity/statement operations."""
        return get_entities(self.name, self.uri)

    def get_documents(self) -> DocumentRepository:
        """Document metadata operations."""
        return get_documents(self.name, self.uri)

    def get_mappings(self) -> MappingRepository:
        """Mapping configuration storage."""
        return get_mappings(self.name, self.uri)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def exists(self) -> bool:
        """Check if dataset exists (has config.yml)."""
        return self._store.exists(path.CONFIG)

    def ensure(self, **data: Any) -> None:
        """Ensure dataset exists, create config.yml if needed.

        Args:
            **data: Initial config data recorded at creation (e.g.
                ``shards=8`` for a huge dataset). Ignored when the dataset
                already exists.
        """
        if not self.exists():
            self.update_model(**data)
            self._log.info("Created dataset")
