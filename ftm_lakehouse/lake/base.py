from functools import cache, cached_property
from pathlib import Path
from typing import Generator, Generic, Type, TypeVar

import yaml
from anystore.logging import get_logger
from anystore.store import BaseStore, get_store
from anystore.types import SDict, Uri
from anystore.util import dict_merge, ensure_uri, join_uri
from ftmq.model import Dataset, DatasetStats

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.decorators import skip_if_latest, versioned
from ftm_lakehouse.exceptions import ImproperlyConfigured
from ftm_lakehouse.lake.archive import DatasetLakeArchive
from ftm_lakehouse.lake.crud import DatasetCruds
from ftm_lakehouse.lake.entities import DatasetEntities
from ftm_lakehouse.lake.fragments import DatasetFragments
from ftm_lakehouse.lake.jobs import DatasetJobs
from ftm_lakehouse.lake.mixins import LakeMixin
from ftm_lakehouse.lake.statements import DatasetStatements
from ftm_lakehouse.model import CatalogModel, DatasetModel

log = get_logger(__name__)

DM = TypeVar("DM", bound=DatasetModel)


class Lakehouse(Generic[DM], LakeMixin):
    """
    FollowTheMoney Data Lakehouse that holds one or more
    [datasets][ftm_lakehouse.base.DatasetLakehouse]
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
        Get a [DatasetLakehouse][ftm_lakehouse.base.DatasetLakehouse] instance
        for the given dataset name.

        Args:
            name: Name of the dataset (also known as `foreign_id`)

        Returns:
            The configured DatasetLakehouse for this dataset name
        """
        storage = get_store(join_uri(self.storage.uri, name))
        config = load_config(storage, name=name, **data)
        if config["name"] != name:
            raise ImproperlyConfigured(
                "Invalid dataset name in config: ",
                f"`{config['name']}` (should be: `{name}`)",
            )

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
    def __init__(self, dataset_model: Type[DM], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.dataset_model = dataset_model

    def exists(self) -> bool:
        """Dataset exists with config.yml"""
        return self.storage.exists(path.CONFIG)

    def ensure(self) -> None:
        """Ensure existence."""
        if self.storage.exists(path.CONFIG):
            return
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
        if "name" in data and data["name"] != self.name:
            raise ImproperlyConfigured(
                "Invalid dataset name: ",
                f"`{data['name']}` (should be: `{self.name}`)",
            )
        return self.load_model(**data)

    def get_statistics(self) -> DatasetStats:
        key = path.STATISTICS
        if self.storage.exists(key):
            return self.storage.get(key, model=DatasetStats)
        self.statements.export_statistics()
        return self.storage.get(key, model=DatasetStats)

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
    def archive(self) -> DatasetLakeArchive:
        """Get the file archive"""
        return DatasetLakeArchive(name=self.name, uri=self.storage.uri)

    @cached_property
    def entities(self) -> DatasetEntities:
        """Get the entities store"""
        return DatasetEntities(name=self.name, uri=self.storage.uri)

    @cached_property
    def statements(self) -> DatasetStatements:
        """Get the statements store"""
        return DatasetStatements(name=self.name, uri=self.storage.uri)

    @cached_property
    def fragments(self) -> DatasetFragments:
        """Get the fragments store"""
        return DatasetFragments(
            statements=self.statements, name=self.name, uri=self.storage.uri
        )

    @cached_property
    def cruds(self) -> DatasetCruds:
        """Crud operations interface"""
        return DatasetCruds(name=self.name, uri=self.storage.uri)

    @cached_property
    def jobs(self) -> DatasetJobs:
        """Job status result storage interface"""
        return DatasetJobs(name=self.name, uri=self.storage.uri)

    @skip_if_latest(path.INDEX, [tag.STATEMENTS_UPDATED, tag.FRAGMENTS_UPDATED])
    def make(self) -> None:
        """
        Run a full update for the dataset:
        - Flush fragments into statement store
        - Export statements.csv
        - Export statistics.json
        - Export entities.ftm.json
        - Export index.json
        """
        self.fragments.flush()
        self.statements.export()
        self.statements.export_statistics()
        self.entities.export()
        self.make_index()


@cache
def get_lakehouse(
    uri: Uri | None = None, dataset_model: Type[DM] | None = None, **kwargs
) -> Lakehouse:
    """
    Get a [FollowTheMoney Data Lakehouse][ftm_lakehouse.base.Lakehouse]. If
    `uri` is set, use this instead of the globally settings uri. Optionally pass
    through settings via **kwargs.

    Args:
        uri: Base path to lakehouse storage
        kwargs: Optional settings to override

    Returns:
        lakehouse
    """
    from ftm_lakehouse.settings import Settings

    settings = Settings()

    storage = get_store(ensure_uri(uri or settings.uri))
    log.info("Loading lakehouse ...", uri=storage.uri)
    config = load_config(storage, **kwargs)
    return Lakehouse(dataset_model=dataset_model or DatasetModel, **config)


@cache
def get_dataset(
    name: str, dataset_model: Type[DM] | None = None, **data
) -> DatasetLakehouse[DM]:
    """
    Get the lakehouse [Dataset][ftm_lakehouse.base.DatasetLakehouse] from the
    optionally given `uri` or the globally configured
    [Lakehouse][ftm_lakehouse.base.Lakehouse]

    Args:
        name: Name of the dataset (also known as `foreign_id`)
        uri: Path to a `config.yml`, `index.json` or its parent base path.
        data: Additional data to override

    Returns:
        dataset
    """
    lake = get_lakehouse(dataset_model=dataset_model or DatasetModel)
    return lake.get_dataset(name, **data)


@cache
def get_archive(name: str) -> DatasetLakeArchive:
    dataset = get_dataset(name)
    return dataset.archive


@cache
def get_statements(name: str) -> DatasetStatements:
    dataset = get_dataset(name)
    return dataset.statements


@cache
def get_cruds(name: str) -> DatasetCruds:
    dataset = get_dataset(name)
    return dataset.cruds


def load_config(storage: BaseStore, **data) -> SDict:
    """
    Load a catalog or dataset configuration.

    Args:
        storage: Base storage to load config from
        data: Additional data to override

    Returns:
        data
    """
    if storage.exists(path.CONFIG):
        config = storage.get(path.CONFIG, deserialization_func=yaml.safe_load)
    else:
        config = {"name": data.get("name") or "catalog"}
    config = dict_merge(config, data)
    config["uri"] = storage.uri
    return config
