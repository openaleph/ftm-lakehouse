"""
High-level data input/output streaming shorthand functions to use in other
applications (like OpenAleph)
"""

from io import BytesIO
from pathlib import Path
from typing import ContextManager, Iterable, Type, TypeAlias

from anystore.types import BytesGenerator, Uri
from followthemoney import E, EntityProxy, StatementEntity
from ftmq.model import Dataset
from ftmq.store.fragments.loader import BulkLoader
from ftmq.store.lake import LakeWriter
from ftmq.types import StatementEntities, ValueEntities
from ftmq.util import ensure_entity

from ftm_lakehouse.lake.base import DM, DatasetLakehouse, get_dataset
from ftm_lakehouse.model import File

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


def get_entity(
    dataset: DS, entity_id: str, include_fragments: bool | None = False
) -> StatementEntity | None:
    """
    Retrieve an entity from the [deltatable](./interfaces/statements.md),
    optionally patched by its [intermediate
    fragments](./interfaces/fragments.md) data

    Args:
        dataset: The dataset
        entity_id: The ID of the Entity
        include_fragments: Whether or not patch the Entity with its current fragments

    Returns:
        An Entity or None
    """
    dataset = ensure_dataset(dataset)
    fragment = None
    entity = dataset.statements.get_entity(entity_id)
    if include_fragments:
        fragment = dataset.fragments.get_entity(entity_id)
        if fragment:
            fragment = ensure_entity(fragment, StatementEntity, dataset.name)
        if fragment and entity:
            entity = entity.merge(fragment)
    return entity or fragment


def entity_writer(dataset: DS, origin: str) -> ContextManager[LakeWriter]:
    dataset = ensure_dataset(dataset)
    return dataset.statements.bulk(origin)


def fragments_writer(dataset: DS, origin: str) -> ContextManager[BulkLoader]:
    dataset = ensure_dataset(dataset)
    return dataset.fragments.bulk(origin)


def write_entities(
    dataset: DS, entities: Iterable[E], origin: str, update: bool | None = False
) -> int:
    i = 0
    dataset = ensure_dataset(dataset)
    with entity_writer(dataset, origin) as bulk:
        for e in entities:
            bulk.add_entity(e)
            i += 1
    if update:
        dataset.make()
    return i


def write_fragment(
    dataset: DS,
    entity: EntityProxy,
    origin: str | None = None,
    fragment: str | None = None,
) -> None:
    dataset = ensure_dataset(dataset)
    dataset.fragments.store.put(entity, fragment=fragment, origin=origin)


def write_fragments(
    dataset: DS, fragments: Iterable[E], origin: str, flush: bool | None = False
) -> int:
    i = 0
    dataset = ensure_dataset(dataset)
    bulk = dataset.fragments.store.bulk()
    for fragment in fragments:
        bulk.put(fragment, origin=origin)
        i += 1
    bulk.flush()
    if flush:
        flush_fragments(dataset)
    return i


def iterate_fragments(dataset: DS) -> ValueEntities:
    dataset = ensure_dataset(dataset)
    yield from dataset.fragments.store.iterate()


def flush_fragments(dataset: DS, origin: str | None = None) -> None:
    dataset = ensure_dataset(dataset)
    dataset.fragments.flush(origin)


def stream_entities(dataset: DS) -> ValueEntities:
    dataset = ensure_dataset(dataset)
    yield from dataset.entities.iterate()


def iterate_entities(
    dataset: DS,
    entity_ids: Iterable[str] | None = None,
    origin: str | None = None,
    bucket: str | None = None,
) -> StatementEntities:
    dataset = ensure_dataset(dataset)
    yield from dataset.statements.iterate(
        entity_ids=entity_ids, origin=origin, bucket=bucket
    )


def lookup_file(dataset: DS, content_hash: str) -> File | None:
    dataset = ensure_dataset(dataset)
    return dataset.archive.lookup_file(content_hash)


def stream_file(dataset: DS, content_hash: str) -> BytesGenerator | None:
    dataset = ensure_dataset(dataset)
    file = lookup_file(dataset, content_hash)
    if file is not None:
        yield from dataset.archive.stream_file(file)


def archive_file(dataset: DS, uri: Uri) -> File:
    dataset = ensure_dataset(dataset)
    return dataset.archive.archive_file(uri)


def archive_local_path(dataset: DS, content_hash: str) -> ContextManager[Path]:
    dataset = ensure_dataset(dataset)
    file = dataset.archive.lookup_file(content_hash)
    return dataset.archive.local_path(file)


def archive_open_file(dataset: DS, content_hash: str) -> ContextManager[BytesIO]:
    dataset = ensure_dataset(dataset)
    file = dataset.archive.lookup_file(content_hash)
    return dataset.archive.open_file(file)


def get_dataset_metadata(dataset: DS, dataset_model: Type[DM] | None = None) -> DM:
    dataset = ensure_dataset(dataset, dataset_model=dataset_model)
    return dataset.model


def update_dataset_metadata(
    dataset: DS, dataset_model: Type[DM] | None = None, **data
) -> DM:
    dataset = ensure_dataset(dataset, dataset_model=dataset_model)
    return dataset.make_config(**data)


def has_dataset(dataset: DS) -> bool:
    dataset = ensure_dataset(dataset, ensure=False)
    return dataset.exists()
