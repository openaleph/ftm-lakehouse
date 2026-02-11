"""Special tweaks if the lakehouse is local path running on a zfs"""

import subprocess
from dataclasses import dataclass, field
from functools import cache

from anystore.logic.uri import uri_to_path
from anystore.types import Uri

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
    compression="zstd",
    sync="disabled",
)

STATEMENTS = DatasetConfig(
    recordsize="1M",
    compression="off",
    sync="standard",
)

PARENT_PROPS = {
    "atime": "off",
    "xattr": "sa",
    "dnodesize": "auto",
}


def zfs_create(
    dataset: str, props: dict[str, str] | None = None, exist_ok: bool = True
):
    cmd = ["zfs", "create"]
    for k, v in (props or {}).items():
        cmd.extend(["-o", f"{k}={v}"])
    cmd.append(dataset)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if exist_ok and "dataset already exists" in result.stderr:
            return
        raise RuntimeError(f"zfs create failed: {result.stderr.strip()}")


@cache
def ensure_zfs_dataset(lake_uri: Uri, dataset: str):
    base = uri_to_path(lake_uri)
    base = f"{base}/{dataset}".lstrip("/")
    zfs_create(base, PARENT_PROPS)
    zfs_create(f"{base}/{path.ARCHIVE}", ARCHIVE.to_props())
    zfs_create(f"{base}/{path.STATEMENTS}", STATEMENTS.to_props())
