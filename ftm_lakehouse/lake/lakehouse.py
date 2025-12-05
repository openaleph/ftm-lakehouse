"""
Lakehouse orchestration layer.

Provides the main Lakehouse and DatasetLakehouse classes that wire together
storage, logic, and facade components.
"""

from functools import cached_property
from pathlib import Path
from typing import Any, Generator, Generic, Type, TypeVar

from anystore.functools import weakref_cache as cache
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import ensure_uri, join_uri
from ftmq.model import Dataset, DatasetStats

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.core.config import load_config
from ftm_lakehouse.core.decorators import skip_if_latest, versioned
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.lake.entities import DatasetEntities
from ftm_lakehouse.lake.mappings import DatasetMappings
from ftm_lakehouse.model import CatalogModel, DatasetModel
from ftm_lakehouse.service import DatasetArchive, DatasetJobs
from ftm_lakehouse.util import check_dataset

log = get_logger(__name__)

DM = TypeVar("DM", bound=DatasetModel)


class Lakehouse(Generic[DM], LakeMixin):
    """
    FollowTheMoney Data Lakehouse that holds one or more datasets.

    The Lakehouse is the top-level orchestration class that manages
    multiple datasets and their configurations.
    """

    def __init__(self, dataset_model: Type[DM], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.dataset_model = dataset_model

    def load_model(self, **data) -> CatalogModel:
        data["name"] = self.name
        return CatalogModel(**load_config(self.storage, **data))

    @property
    def model(self) -> CatalogModel:
        return self.load_model()

    @versioned(path.CONFIG)
    def make_config(self, **data) -> CatalogModel:
        """
        Get catalog config from existing `config.yml` if it exists, patch it
        with updated `**data` and write it to versioned `config.yml`

        Returns:
            model
        """
        return self.load_model(**data)

    @versioned(path.INDEX)
    def make_index(self) -> CatalogModel:
        """
        Write versioned catalog `index.json`. This could be used as a periodic
        task or after some dataset metadata changes.
        """
        datasets = [Dataset(**d.load_model().model_dump()) for d in self.get_datasets()]
        return self.load_model(datasets=datasets)

    def get_dataset(self, name: str, **data) -> "DatasetLakehouse[DM]":
        """
        Get a DatasetLakehouse instance for the given dataset name.

        Args:
            name: Name of the dataset (also known as `foreign_id`)

        Returns:
            The configured DatasetLakehouse for this dataset name
        """
        storage = get_store(join_uri(self.storage.uri, name))
        config = load_config(storage, name=name, **data)
        config["name"] = check_dataset(name, data)
        return DatasetLakehouse(dataset_model=self.dataset_model, **config)

    def get_datasets(self) -> Generator["DatasetLakehouse[DM]", None, None]:
        """
        Iterate through the datasets.

        Yields:
            The dataset instances that have a `config.yml`
        """
        for child in self.storage._fs.ls(self.storage.uri):
            dataset = Path(child).name
            if self.storage.exists(f"{dataset}/{path.CONFIG}"):
                yield self.get_dataset(dataset)


class DatasetLakehouse(Generic[DM], LakeMixin):
    """
    A single dataset within the lakehouse.

    Provides unified access to all dataset operations through facades:

    - archive: File storage
    - entities: Entity read/write operations
    - jobs: Job status tracking
    - mappings: CSV mapping transformations
    """

    def __init__(self, dataset_model: Type[DM], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.dataset_model = dataset_model

    def exists(self) -> bool:
        """Dataset exists with config.yml"""
        return self.storage.exists(path.CONFIG)

    def ensure(self) -> None:
        """Ensure existence."""
        if not self.exists():
            self.make_config()

    def load_model(self, **data) -> DM:
        data["name"] = self.name
        return self.dataset_model(**load_config(self.storage, **data))

    @property
    def model(self) -> DM:
        return self.load_model()

    @versioned(path.CONFIG)
    def make_config(self, **data) -> DM:
        """
        Get dataset config from existing `config.yml` if it exists, patch it
        with updated `**data` and write it to versioned `config.yml`

        Returns:
            model
        """
        data["name"] = check_dataset(self.name, data)
        return self.load_model(**data)

    def get_statistics(self) -> DatasetStats:
        return self.entities.get_statistics()

    @versioned(path.INDEX)
    def make_index(self, compute_stats: bool | None = False) -> Dataset:
        """
        Recompute the `index.json` and write it versioned.

        Args:
            compute_stats: Compute dataset statistics and write out updated
                `statistics.json` for each dataset.

        Returns:
            model
        """
        # ensure only Dataset data (not subclassed extra user data)
        dataset = Dataset(**self.load_model().model_dump())
        if compute_stats:
            dataset.apply_stats(self.get_statistics())
        return dataset

    @cached_property
    def archive(self) -> DatasetArchive:
        """Get the file archive"""
        return DatasetArchive(name=self.name, uri=self.storage.uri)

    @cached_property
    def entities(self) -> DatasetEntities:
        """Get the entities interface (unified entity/statement operations)"""
        return DatasetEntities(name=self.name, uri=self.storage.uri)

    @cached_property
    def jobs(self) -> DatasetJobs:
        """Job status result storage interface"""
        return DatasetJobs(name=self.name, uri=self.storage.uri)

    @cached_property
    def mappings(self) -> DatasetMappings:
        """Get the mappings interface for CSV/tabular data transformations"""
        return DatasetMappings(
            archive=self.archive,
            entities=self.entities,
            name=self.name,
            uri=self.storage.uri,
        )

    @skip_if_latest(path.INDEX, [tag.STATEMENTS_UPDATED, tag.JOURNAL_UPDATED])
    def make(self) -> None:
        """
        Run a full update for the dataset:

        - Flush journal into statement store
        - Export statements.csv
        - Export statistics.json
        - Export entities.ftm.json
        - Export index.json
        """
        self.ensure()
        self.entities.flush()
        self.entities.export_statements()
        self.entities.export_statistics()
        self.entities.export()
        self.make_index()


@cache
def get_lakehouse(
    uri: Uri | None = None, dataset_model: Type[DM] | None = None, **kwargs: Any
) -> Lakehouse:
    """
    Get a FollowTheMoney Data Lakehouse.

    If `uri` is set, use this instead of the globally configured uri.
    Optionally pass through settings via **kwargs.

    Args:
        uri: Base path to lakehouse storage
        **kwargs: Optional settings to override

    Returns:
        lakehouse
    """
    from ftm_lakehouse.core.settings import Settings

    settings = Settings()

    storage = get_store(ensure_uri(uri or settings.uri))
    log.info("Loading lakehouse ...", uri=storage.uri)
    config = load_config(storage, **kwargs)
    return Lakehouse(dataset_model=dataset_model or DatasetModel, **config)


@cache
def get_dataset(
    name: str, dataset_model: Type[DM] | None = None, **data: Any
) -> DatasetLakehouse[DM]:
    """
    Get a dataset from the globally configured Lakehouse.

    Args:
        name: Name of the dataset (also known as `foreign_id`)
        dataset_model: Pydantic model for dataset metadata
        **data: Additional data to pass to the dataset

    Returns:
        dataset
    """
    lake = get_lakehouse(dataset_model=dataset_model or DatasetModel)
    return lake.get_dataset(name, **data)


@cache
def get_archive(name: str) -> DatasetArchive:
    """Get the archive for a dataset."""
    dataset = get_dataset(name)
    return dataset.archive


@cache
def get_entities(name: str) -> DatasetEntities:
    """Get the entities interface for a dataset."""
    dataset = get_dataset(name)
    return dataset.entities


@cache
def get_mappings(name: str) -> DatasetMappings:
    """Get the mappings interface for a dataset."""
    dataset = get_dataset(name)
    return dataset.mappings
