"""ZFS integration for container and local deployments."""

from ftm_lakehouse.core.zfs.agent import (
    handle_connection,
    handle_request,
    validate_dataset,
)
from ftm_lakehouse.core.zfs.helpers import (
    ARCHIVE,
    PARENT_PROPS,
    STATEMENTS,
    DatasetConfig,
    ensure_zfs_dataset,
    zfs_create,
    zfs_create_local,
    zfs_create_socket,
)

__all__ = [
    "ARCHIVE",
    "DatasetConfig",
    "PARENT_PROPS",
    "STATEMENTS",
    "ensure_zfs_dataset",
    "handle_connection",
    "handle_request",
    "validate_dataset",
    "zfs_create",
    "zfs_create_local",
    "zfs_create_socket",
]
