"""Lakehouse-specific ZFS dataset provisioning.

Only the per-storage-type tuning and the client-side caller live here – the
transport (local ``zfs`` subprocess vs. socket agent, mountpoint chown, peer
authentication) is the external `zfs-agent
<https://github.com/dataresearchcenter/zfs-agent>`_ package: configure it via
its ``ZFS_SOCKET`` / ``ZFS_OWNER`` environment, run the host-side agent with
its ``zfs-agent`` command.
"""

from dataclasses import dataclass, field
from functools import cache

from followthemoney.dataset.util import dataset_name_check
from zfs_agent import zfs_create

from ftm_lakehouse.core.conventions import path


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
    recordsize="128K",
    compression="zstd-9",
)

STATEMENTS = DatasetConfig(
    recordsize="1M",
    # Parquet already compresses (ftmq WRITER_SMALL/WRITER_LARGE use ZSTD).
    # ZFS-level compression on top burns CPU per block and almost never
    # shrinks anything further on high-entropy parquet output.
    compression="off",
)

PARENT_PROPS = {
    "atime": "off",
    "xattr": "sa",
    "dnodesize": "auto",
}


@cache
def ensure_zfs_dataset(pool: str, dataset: str) -> None:
    """Create the dataset's tuned ZFS hierarchy under ``pool`` (idempotent).

    One parent plus one child per storage type, each with its
    `DatasetConfig` properties. Cached per ``(pool, dataset)`` so the
    actual ``zfs create`` calls fire once per process.
    """
    dataset_name_check(dataset)
    base = f"{pool}/{dataset}"
    zfs_create(base, **PARENT_PROPS)
    zfs_create(f"{base}/{path.ARCHIVE}", **ARCHIVE.to_props())
    zfs_create(f"{base}/{path.STATEMENTS}", **STATEMENTS.to_props())
