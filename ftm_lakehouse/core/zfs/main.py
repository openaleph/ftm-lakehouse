"""A lakehouse dataset's ZFS layout: tuning, parts, and the calls on them.

One lakehouse dataset is one parent ZFS dataset (``base``) plus a tuned
child per storage type. Everything here addresses datasets through that
layout and talks to ``zfs-agent`` for them – create, status, snapshot,
check before a receive, abort; the stream itself is `stream`'s.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import cache

from zfs_agent import Status, zfs_abort, zfs_create, zfs_snapshot, zfs_status

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.zfs.plan import check_target
from ftm_lakehouse.util import validate_dataset_name


@dataclass
class DatasetConfig:
    recordsize: str = "128K"
    compression: str = "zstd"
    sync: str = "standard"
    logbias: str = "throughput"
    extra: dict[str, str] = field(default_factory=dict)

    def to_props(self) -> dict[str, str]:
        return {
            "recordsize": self.recordsize,
            "compression": self.compression,
            "sync": self.sync,
            "logbias": self.logbias,
            **self.extra,
        }


ARCHIVE = DatasetConfig(
    recordsize="1M",
    compression="zstd-9",
)

STATEMENTS = DatasetConfig(
    recordsize="1M",
    compression="off",  # parquet already compresses inline
)

PARENT_PROPS = {
    "atime": "off",
    "xattr": "sa",
    "dnodesize": "auto",
}

BASE = "base"
PARTS = (BASE, path.ARCHIVE, path.STATEMENTS)
"""The ZFS datasets of one lakehouse dataset, parent first – a child can
only be created, or received, once its parent exists."""

PART_PROPS = {
    BASE: PARENT_PROPS,
    path.ARCHIVE: ARCHIVE.to_props(),
    path.STATEMENTS: STATEMENTS.to_props(),
}
"""Tuning per part: set on creation, and on receive – streams carry no
properties."""

SNAPSHOT_FORMAT = "%Y%m%d%H%M%S"

DatasetStatus = dict[str, Status]
"""`zfs_agent.Status` per part of a dataset."""


def zfs_dataset(pool: str, dataset: str, part: str = BASE) -> str:
    """ZFS dataset path of one part of a lakehouse dataset.

    Raises:
        ValueError: for an invalid or reserved dataset name, or an unknown
            part.
    """
    validate_dataset_name(dataset)
    if part not in PARTS:
        raise ValueError(f"Unknown ZFS part: `{part}`")
    base = f"{pool}/{dataset}"
    return base if part == BASE else f"{base}/{part}"


@cache
def ensure_zfs_dataset(pool: str, dataset: str) -> None:
    """Create the dataset's tuned ZFS hierarchy under ``pool`` (idempotent).

    One parent plus one child per storage type, each with its
    `DatasetConfig` properties. Cached per ``(pool, dataset)`` so the
    actual ``zfs create`` calls fire once per process.
    """
    for part in PARTS:
        zfs_create(zfs_dataset(pool, dataset, part), **PART_PROPS[part])


def select_parts(archive: bool = True, statements: bool = True) -> list[str]:
    """The parts to replicate – ``base`` always, the children optionally."""
    selected = {BASE: True, path.ARCHIVE: archive, path.STATEMENTS: statements}
    return [part for part in PARTS if selected[part]]


def dataset_status(pool: str, dataset: str) -> DatasetStatus:
    """Snapshots and pending resume token of every part."""
    return {part: zfs_status(zfs_dataset(pool, dataset, part)) for part in PARTS}


def snapshot_dataset(
    pool: str, dataset: str, parts: Iterable[str], name: str | None = None
) -> str:
    """Snapshot ``parts`` atomically, named by the current UTC time by default.

    Only the parts about to be sent: a part's newest snapshot is then the
    last one it was sent at, whichever parts a replication left out.

    Returns:
        The snapshot name (after the ``@``).
    """
    name = name or datetime.now(timezone.utc).strftime(SNAPSHOT_FORMAT)
    zfs_snapshot(*(f"{zfs_dataset(pool, dataset, part)}@{name}" for part in parts))
    return name


def check_receive(
    pool: str,
    dataset: str,
    part: str,
    base: str | None,
    force: bool = False,
    replace: bool = False,
) -> str:
    """`check_target` a part as it is right now.

    Returns:
        The part's ZFS dataset, cleared to receive.
    """
    target = zfs_dataset(pool, dataset, part)
    check_target(zfs_status(target), base, force, replace)
    return target


def abort_receive(pool: str, dataset: str, part: str) -> None:
    """Discard the partial state of an interrupted receive into a part."""
    zfs_abort(zfs_dataset(pool, dataset, part))
