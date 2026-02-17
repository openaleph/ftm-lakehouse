"""ZFS socket agent: server-side request validation and handling."""

import re
import socket

import orjson
from anystore.logging import get_logger
from followthemoney.dataset.util import dataset_name_check

from ftm_lakehouse.core.zfs.helpers import zfs_create_local

log = get_logger(__name__)


_ZFS_COMPONENT_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def validate_dataset(dataset: str, allowed_pool: str | None) -> str | None:
    """Validate a ZFS dataset path. Returns an error string or None if valid.

    Only the leaf (last) path component is checked with ``dataset_name_check``
    (FTM dataset naming rules).  Parent components use permissive ZFS naming
    rules (alphanumeric, hyphens, dots, underscores).
    """
    if not dataset:
        return "empty dataset name"
    if ".." in dataset:
        return f"path traversal not allowed: {dataset!r}"

    parts = dataset.split("/")

    # Parent components: permissive ZFS naming
    for part in parts[:-1]:
        if not _ZFS_COMPONENT_RE.match(part):
            return f"invalid path component: {part!r}"

    # Leaf component: strict FTM dataset name
    leaf = parts[-1]
    try:
        dataset_name_check(leaf)
    except ValueError:
        return f"invalid dataset name: {leaf!r}"

    if allowed_pool and not dataset.startswith(allowed_pool):
        return f"dataset {dataset!r} not under pool {allowed_pool!r}"
    return None


def handle_request(
    data: dict, allowed_pool: str | None, owner: str | None = None
) -> dict:
    """Process a single JSON request and return a response dict."""
    action = data.get("action")
    if action != "create":
        log.warning("Unknown action requested", action=action)
        return {"ok": False, "error": f"unknown action: {action!r}"}

    dataset = data.get("dataset", "")
    err = validate_dataset(dataset, allowed_pool)
    if err:
        log.warning("Dataset validation failed", dataset=dataset, error=err)
        return {"ok": False, "error": err}

    props = data.get("props") or {}
    if not isinstance(props, dict):
        return {"ok": False, "error": "props must be a dict"}

    log.info("Creating ZFS dataset", dataset=dataset, props=props, owner=owner)
    try:
        zfs_create_local(dataset, props, exist_ok=True, owner=owner)
    except RuntimeError as e:
        log.error("zfs create failed", dataset=dataset, error=str(e))
        return {"ok": False, "error": str(e)}

    log.info("ZFS dataset created", dataset=dataset)
    return {"ok": True}


def handle_connection(
    conn: socket.socket, allowed_pool: str | None, owner: str | None = None
) -> None:
    """Read one JSON line from a connection, process it, write the response."""
    try:
        line = conn.makefile().readline()
        if not line:
            log.debug("Empty request, closing connection")
            return
        try:
            data = orjson.loads(line)
        except orjson.JSONDecodeError as e:
            log.warning("Received invalid JSON", error=str(e))
            response = {"ok": False, "error": f"invalid JSON: {e}"}
        else:
            response = handle_request(data, allowed_pool, owner)
        conn.sendall(orjson.dumps(response) + b"\n")
    finally:
        conn.close()
