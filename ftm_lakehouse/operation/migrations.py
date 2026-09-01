"""Dataset migrations – one function per storage-layout change, applied once.

A migration takes the dataset's ``DatasetRef`` and resolves what it needs
through the repository factories. `MIGRATIONS` lists them oldest first;
[`MigrateOperation`][ftm_lakehouse.operation.maintenance.MigrateOperation] runs
the ones a dataset has not seen and stamps
[`tag.migration`][ftm_lakehouse.core.conventions.tag.migration] with the
function's name – so the function name is the migration id.

Migrations are forward-only (no down-migration, no compatibility shim in the
read path) and idempotent (``--force`` re-runs the whole registry).
"""

from typing import Callable

from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.factories import get_entities

Migration = Callable[[DatasetRef], None]
"""A migration: a function of the dataset address, run for its effect."""


def migrate_parquet_add_role(ref: DatasetRef) -> None:
    """Add the ``role`` column to a statement store that predates it.

    Metadata-only schema evolution
    ([`evolve_schema`][ftm_lakehouse.repository.EntityRepository.evolve_schema]):
    the older rows read back ``role IS NULL``, which is the "no role" case, so
    row identity is what it was before the column existed and no re-merge is
    owed.
    """
    get_entities(*ref).evolve_schema()


MIGRATIONS: tuple[Migration, ...] = (migrate_parquet_add_role,)
"""Every migration, oldest first – the order they are applied in."""
