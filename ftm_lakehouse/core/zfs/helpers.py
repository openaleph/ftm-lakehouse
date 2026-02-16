"""ZFS dataset creation helpers: config, subprocess, socket client, and dispatch."""

import socket
import subprocess
from dataclasses import dataclass, field
from functools import cache

import orjson
from anystore.logging import get_logger
from anystore.logic.uri import uri_to_path
from anystore.types import Uri

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings

log = get_logger(__name__)


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
    compression="lz4",
    sync="standard",
)

PARENT_PROPS = {
    "atime": "off",
    "xattr": "sa",
    "dnodesize": "auto",
}


def zfs_create_local(
    dataset: str, props: dict[str, str] | None = None, exist_ok: bool = True
):
    """Create a ZFS dataset via local subprocess."""
    log.info("Creating ZFS dataset (local)", dataset=dataset, props=props)
    cmd = ["zfs", "create"]
    for k, v in (props or {}).items():
        cmd.extend(["-o", f"{k}={v}"])
    cmd.append(dataset)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if exist_ok and "dataset already exists" in result.stderr:
            log.debug("ZFS dataset already exists", dataset=dataset)
            return
        log.error("zfs create failed", dataset=dataset, error=result.stderr.strip())
        raise RuntimeError(f"zfs create failed: {result.stderr.strip()}")


def zfs_create_socket(
    socket_path: str, dataset: str, props: dict[str, str] | None = None
):
    """Send a ``zfs create`` request to a remote agent over a Unix socket."""
    log.debug("Requesting zfs create via socket", socket=socket_path, dataset=dataset)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(socket_path)
        request = orjson.dumps(
            {"action": "create", "dataset": dataset, "props": props or {}}
        )
        sock.sendall(request + b"\n")
        response = orjson.loads(sock.makefile().readline())
        if not response.get("ok"):
            error = response.get("error", "unknown")
            log.error("Socket zfs create failed", dataset=dataset, error=error)
            raise RuntimeError(f"zfs create failed: {error}")


def zfs_create(
    dataset: str, props: dict[str, str] | None = None, exist_ok: bool = True
):
    """Create a ZFS dataset, dispatching to socket or local subprocess."""
    settings = Settings()
    if settings.zfs_socket:
        return zfs_create_socket(settings.zfs_socket, dataset, props)
    return zfs_create_local(dataset, props, exist_ok)


@cache
def ensure_zfs_dataset(lake_uri: Uri, dataset: str):
    base = uri_to_path(lake_uri)
    base = f"{base}/{dataset}".lstrip("/")
    zfs_create(base, PARENT_PROPS)
    zfs_create(f"{base}/{path.ARCHIVE}", ARCHIVE.to_props())
    zfs_create(f"{base}/{path.STATEMENTS}", STATEMENTS.to_props())
