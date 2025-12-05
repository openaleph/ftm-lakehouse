"""
High-level data input/output streaming shorthand functions to use in other
applications (like [OpenAleph](https://openaleph.org))

The purpose is to use this functional approach in applications:

```python
from ftm_lakehouse import io

# get an entity
entity = io.get_entity("test_dataset", "id-123")

# open a file for local processing
with io.open_file("test_dataset", checksum) as fh:
    _external_process(fh.read())

# import entities via bulk writer
io.write_entities("test_dataset", entities)
```
"""

from pathlib import Path
from typing import IO, ContextManager, Iterable, Type, TypeAlias

from anystore.types import BytesGenerator, Uri
from followthemoney import E, EntityProxy, StatementEntity
from ftmq.model import Dataset
from ftmq.types import StatementEntities, ValueEntities

from ftm_lakehouse.lake import DM, DatasetLakehouse, get_dataset
from ftm_lakehouse.model import File
from ftm_lakehouse.service import JournalWriter

DS: TypeAlias = str | Dataset | DatasetLakehouse


def ensure_dataset(
    dataset: DS, ensure: bool | None = True, dataset_model: Type[DM] | None = None
) -> DatasetLakehouse[DM]:
    if isinstance(dataset, str):
        dataset = get_dataset(dataset, dataset_model=dataset_model)
    if isinstance(dataset, Dataset):
        dataset = get_dataset(dataset.name, dataset_model=dataset_model)
    if ensure:
        dataset.ensure()
    return dataset


# -----------------------------------------------------------------------------
# Entity operations
# -----------------------------------------------------------------------------


def get_entity(dataset: DS, entity_id: str) -> StatementEntity | None:
    """
    Retrieve an entity by ID.

    Args:
        dataset: Dataset name or Dataset class / model
        entity_id: The ID of the Entity

    Returns:
        An Entity or None
    """
    dataset = ensure_dataset(dataset)
    return dataset.entities.get(entity_id)


def entity_writer(
    dataset: DS, origin: str | None = None
) -> ContextManager[JournalWriter]:
    """Get a bulk writer for adding entities."""
    dataset = ensure_dataset(dataset)
    return dataset.entities.bulk(origin)


def write_entities(
    dataset: DS,
    entities: Iterable[E],
    origin: str | None = None,
    update: bool | None = False,
) -> int:
    """Write entities to the dataset."""
    i = 0
    dataset = ensure_dataset(dataset)
    with entity_writer(dataset, origin) as bulk:
        for e in entities:
            bulk.add_entity(e)
            i += 1
    if update:
        dataset.make()
    return i


def write_entity(
    dataset: DS,
    entity: EntityProxy,
    origin: str | None = None,
) -> None:
    """Write a single entity."""
    dataset = ensure_dataset(dataset)
    dataset.entities.add(entity, origin)


def flush(dataset: DS) -> int:
    """Flush pending writes to storage."""
    dataset = ensure_dataset(dataset)
    return dataset.entities.flush()


def stream_entities(dataset: DS) -> ValueEntities:
    """Stream all entities from the exported JSON file."""
    dataset = ensure_dataset(dataset)
    yield from dataset.entities.iterate()


def iterate_entities(
    dataset: DS,
    entity_ids: Iterable[str] | None = None,
    origin: str | None = None,
    bucket: str | None = None,
) -> StatementEntities:
    """Query entities from the statement store."""
    dataset = ensure_dataset(dataset)
    yield from dataset.entities.query(
        entity_ids=entity_ids, origin=origin, bucket=bucket
    )


# -----------------------------------------------------------------------------
# File archive operations
# -----------------------------------------------------------------------------


def lookup_file(dataset: DS, content_hash: str) -> File | None:
    dataset = ensure_dataset(dataset)
    return dataset.archive.lookup_file(content_hash)


def stream_file(dataset: DS, content_hash: str) -> BytesGenerator | None:
    dataset = ensure_dataset(dataset)
    file = lookup_file(dataset, content_hash)
    if file is not None:
        yield from dataset.archive.stream_file(file)


def open_file(dataset: DS, content_hash: str) -> ContextManager[IO[bytes]]:
    dataset = ensure_dataset(dataset)
    file = dataset.archive.lookup_file(content_hash)
    return dataset.archive.open_file(file)


def archive_file(dataset: DS, uri: Uri) -> File:
    dataset = ensure_dataset(dataset)
    return dataset.archive.archive_file(uri)


def get_local_path(dataset: DS, content_hash: str) -> ContextManager[Path]:
    dataset = ensure_dataset(dataset)
    file = dataset.archive.lookup_file(content_hash)
    return dataset.archive.local_path(file)


# -----------------------------------------------------------------------------
# Dataset metadata operations
# -----------------------------------------------------------------------------


def get_dataset_metadata(dataset: DS, dataset_model: Type[DM] | None = None) -> DM:
    dataset = ensure_dataset(dataset, dataset_model=dataset_model)
    return dataset.model


def update_dataset_metadata(
    dataset: DS, dataset_model: Type[DM] | None = None, **data
) -> DM:
    dataset = ensure_dataset(dataset, dataset_model=dataset_model)
    return dataset.make_config(**data)


def exists(dataset: DS) -> bool:
    """
    Test if the given dataset exists in the lake.

    Args:
        dataset: Dataset name or Dataset class / model

    Returns:
        Existence
    """
    dataset = ensure_dataset(dataset, ensure=False)
    return dataset.exists()
