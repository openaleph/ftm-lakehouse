"""Catalog class for multi-dataset management."""

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Generic, TypeVar

from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import join_uri

from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model import CatalogModel, DatasetModel
from ftm_lakehouse.storage import TagStore
from ftm_lakehouse.storage.versions import VersionStore

if TYPE_CHECKING:
    from ftm_lakehouse.dataset import Dataset

log = get_logger(__name__)

DM = TypeVar("DM", bound=DatasetModel)


class Catalog(Generic[DM]):
    """
    Multi-dataset lakehouse catalog.

    The Catalog manages multiple datasets within a lakehouse storage location.

    Example:
        ```python
        from ftm_lakehouse import get_catalog

        catalog = get_catalog()

        # List datasets
        for dataset in catalog.list_datasets():
            print(dataset.name)

        # Get a specific dataset
        dataset = catalog.get_dataset("my_dataset")
        ```
    """

    def __init__(
        self,
        uri: Uri,
        model_class: type[DM] = DatasetModel,
    ) -> None:
        self.uri = uri
        self._model_class = model_class
        self._log = log.bind(catalog=uri)

    def __repr__(self) -> str:
        return f"Catalog({self.uri!r})"

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
    # Model access
    # -------------------------------------------------------------------------

    def _load_model(self, **data: Any) -> CatalogModel:
        """Load catalog model from config."""
        return CatalogModel(**load_config(self._store, **data))

    @property
    def model(self) -> CatalogModel:
        """Load and return the catalog model from config.yml."""
        return self._load_model()

    def update_model(self, **data: Any) -> CatalogModel:
        """
        Update config.yml with new data.

        Args:
            **data: Fields to update in the model

        Returns:
            Updated model
        """
        model = self._load_model(**data)
        self._versions.make(path.CONFIG, model)
        return model

    # -------------------------------------------------------------------------
    # Dataset management
    # -------------------------------------------------------------------------

    def get_dataset(self, name: str, **data: Any) -> "Dataset[DM]":
        """
        Get a Dataset instance by name.

        Args:
            name: Dataset name
            **data: Additional config data (auto-saved to config.yml if dataset exists)

        Returns:
            Dataset instance
        """
        from ftm_lakehouse.dataset import Dataset

        dataset_uri = join_uri(self.uri, name)
        dataset = Dataset(
            name=name,
            uri=dataset_uri,
            model_class=self._model_class,
        )

        # Auto-save config if data provided and dataset exists
        if data and dataset.exists():
            dataset.update_model(**data)

        return dataset

    def list_datasets(self) -> Generator["Dataset[DM]", None, None]:
        """
        Iterate through all datasets in the catalog.

        Yields:
            Dataset instances that have a config.yml
        """
        for child in self._store._fs.ls(self.uri):
            dataset_name = Path(child).name
            if self._store.exists(f"{dataset_name}/{path.CONFIG}"):
                yield self.get_dataset(dataset_name)

    def create_dataset(self, name: str, **data: Any) -> "Dataset[DM]":
        """
        Create a new dataset.

        Args:
            name: Dataset name
            **data: Initial config data

        Returns:
            Created Dataset instance
        """
        dataset = self.get_dataset(name, **data)
        dataset.ensure()
        return dataset
