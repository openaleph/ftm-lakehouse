"""Lakehouse-specific ZFS dataset provisioning and replication.

Only the per-storage-type tuning, the replication logic and the client-side
caller live here – the transport (local ``zfs`` subprocess vs. socket agent,
mountpoint chown, peer authentication) is the external `zfs-agent
<https://github.com/dataresearchcenter/zfs-agent>`_ package: configure it via
its ``ZFS_SOCKET`` / ``ZFS_OWNER`` environment, run the host-side agent with
its ``zfs-agent`` command.

Replication moves a dataset between two lakehouse hosts as one ``zfs send``
stream per *part* – the parent (``base``) and its ``archive`` /
``statements`` children – over HTTP: a client pushes to or pulls from the
other host's ``/{dataset}/_api/zfs`` routes. Separate streams rather than
one ``zfs send -R`` so that a part can be left out: a replication stream
received with ``-F`` destroys the datasets it doesn't carry.

- ``main`` – the dataset's ZFS layout (tuning, parts) and the ``zfs-agent``
  calls on it: create, status, snapshot, check before a receive, abort
- ``plan`` – pure decisions: what to send, whether a target may take it
- ``stream`` – the data path, ``zfs send`` / ``receive`` through a buffer
- ``util`` – pipe and queue plumbing underneath ``stream``
- ``client`` – ``push`` / ``pull`` against another host's routes
"""

from ftm_lakehouse.core.zfs.client import pull, push, remote_status
from ftm_lakehouse.core.zfs.main import (
    ARCHIVE,
    BASE,
    PARENT_PROPS,
    PART_PROPS,
    PARTS,
    SNAPSHOT_FORMAT,
    STATEMENTS,
    DatasetConfig,
    DatasetStatus,
    abort_receive,
    check_receive,
    dataset_status,
    ensure_zfs_dataset,
    select_parts,
    snapshot_dataset,
    zfs_dataset,
)
from ftm_lakehouse.core.zfs.plan import (
    Transfer,
    check_target,
    common_base,
    plan_transfer,
)
from ftm_lakehouse.core.zfs.stream import (
    ReceiveFeed,
    SendStream,
    arechunk,
    arun_receive,
    rechunk,
    run_receive,
)
from ftm_lakehouse.core.zfs.util import CHUNK_SIZE

__all__ = [
    "ARCHIVE",
    "BASE",
    "CHUNK_SIZE",
    "PARENT_PROPS",
    "PARTS",
    "PART_PROPS",
    "SNAPSHOT_FORMAT",
    "STATEMENTS",
    "DatasetConfig",
    "DatasetStatus",
    "ReceiveFeed",
    "SendStream",
    "Transfer",
    "abort_receive",
    "arechunk",
    "arun_receive",
    "check_receive",
    "check_target",
    "common_base",
    "dataset_status",
    "ensure_zfs_dataset",
    "plan_transfer",
    "pull",
    "push",
    "rechunk",
    "remote_status",
    "run_receive",
    "select_parts",
    "snapshot_dataset",
    "zfs_dataset",
]
