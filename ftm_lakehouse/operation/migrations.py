"""Dataset migrations – one function per storage-layout change, applied once.

`MIGRATIONS` lists them oldest first; `MigrateOperation` runs the ones a
dataset has not seen and tags each by function name – the migration id.
Forward-only and idempotent.
"""

from typing import Callable

from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.factories import get_entities

Migration = Callable[[DatasetRef], None]
"""A migration: a function of the dataset address, run for its effect."""


def migrate_parquet_add_role(ref: DatasetRef) -> None:
    """Add the ``role`` column to a statement store that predates it –
    metadata-only (`evolve_schema`); old rows read ``role IS NULL`` ("no
    role"), so no re-merge is owed."""
    get_entities(*ref).statements.evolve_schema()


def migrate_parquet_table_properties(ref: DatasetRef) -> None:
    """Bound the Delta log of a statement store created without retention
    properties (`configure_table`): checkpoint, then drop the expired log."""
    get_entities(*ref).statements.configure_table()


MIGRATIONS: tuple[Migration, ...] = (
    migrate_parquet_add_role,
    migrate_parquet_table_properties,
)
"""Every migration, oldest first – the order they are applied in."""
