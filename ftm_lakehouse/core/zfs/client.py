"""The replication client: push to, or pull from, another host's
``/{dataset}/_api/zfs`` routes."""

from collections.abc import Callable
from typing import Any

import httpx
from anystore.logging import get_logger

from ftm_lakehouse.core.api import USER_AGENT
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs.main import (
    PART_PROPS,
    DatasetStatus,
    abort_receive,
    check_receive,
    dataset_status,
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
from ftm_lakehouse.core.zfs.stream import SendStream, run_receive
from ftm_lakehouse.util import validate_dataset_name

log = get_logger(__name__)


def _client(url: str) -> httpx.Client:
    """An http client for a replication peer.

    Deliberately not the lakehouse api client: that one sends
    ``LAKEHOUSE_API_KEY`` to whatever it talks to. A peer only gets
    ``LAKEHOUSE_ZFS_PEER_KEY`` / ``_SECRET``, and only if they're set.
    """
    settings = Settings()
    headers = {"User-Agent": USER_AGENT}
    if settings.zfs_peer_key and settings.zfs_peer_secret:
        headers["X-Api-Key"] = settings.zfs_peer_key
        headers["X-Api-Secret"] = settings.zfs_peer_secret
    # A receive answers once `zfs receive` is done – after the whole stream.
    timeout = httpx.Timeout(6 * 3600, connect=30)
    return httpx.Client(base_url=url.rstrip("/"), headers=headers, timeout=timeout)


def _raise_for_status(response: httpx.Response) -> None:
    """Raise with the server's ``detail`` rather than just the status line."""
    if response.is_error:
        response.read()
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(
            f"{response.status_code} {response.request.method} "
            f"{response.url}: {detail}"
        )


def _request(client: httpx.Client, method: str, url: str, **kwargs: Any) -> Any:
    """One non-streaming request; returns the decoded json body."""
    response = client.request(method, url, **kwargs)
    _raise_for_status(response)
    return response.json()


def _remote_status(client: httpx.Client, dataset: str) -> DatasetStatus:
    status: DatasetStatus = _request(client, "GET", f"/{dataset}/_api/zfs")
    return status


def remote_status(url: str, dataset: str) -> DatasetStatus:
    """Status of every part of ``dataset`` on the host at ``url``."""
    validate_dataset_name(dataset)
    with _client(url) as client:
        return _remote_status(client, dataset)


def _replicate(
    parts: list[str],
    source: Callable[[], DatasetStatus],
    target: Callable[[], DatasetStatus],
    abort: Callable[[str], None],
    transfer: Callable[[str, Transfer], None],
    snapshot: str | Callable[[], str],
    force: bool,
    replace: bool,
) -> str:
    """Bring every part of the target to ``snapshot``, one stream each.

    ``snapshot`` is a name, or a callable that takes a new snapshot and
    returns its name – called only once every part is known to be
    receivable, so a refused run leaves no snapshot behind. Every part is
    planned before the first byte moves, so a refusal leaves the target as
    it was rather than with some parts at the new snapshot. With ``force``,
    interrupted receives are discarded instead of resumed – a token whose
    snapshot is gone can't be resumed at all.

    Returns:
        The name of the snapshot the target now has.
    """
    if force:
        for part, status in target().items():
            if part in parts and status["resume_token"]:
                log.info("Discarding interrupted receive", part=part)
                abort(part)
    sources, targets = source(), target()
    if callable(snapshot):
        # A new snapshot adds nothing the target has, so the common base –
        # and whether the target may receive on top of it – is known now.
        for part in parts:
            base = common_base(sources[part], targets[part])
            check_target(targets[part], base, force, replace)
        snapshot = snapshot()
        sources = source()
    plans = {
        part: plan_transfer(sources[part], targets[part], snapshot, force, replace)
        for part in parts
    }
    for part in parts:
        plan = plans[part]
        if plan is not None and plan.token:
            log.info("Resuming interrupted receive", part=part)
            try:
                transfer(part, plan)
            except RuntimeError as e:
                raise RuntimeError(
                    f"{e} – use force to discard the interrupted receive"
                ) from e
            plan = plan_transfer(
                source()[part], target()[part], snapshot, force, replace
            )
        if plan is None:
            log.info("Up to date", part=part, snapshot=snapshot)
            continue
        log.info(
            "Sending", part=part, snapshot=plan.snapshot, since=plan.since or "(full)"
        )
        transfer(part, plan)
    return snapshot


def push(
    url: str,
    pool: str,
    dataset: str,
    buffer: int,
    archive: bool = True,
    statements: bool = True,
    snapshot: str | None = None,
    force: bool = False,
    replace: bool = False,
    prepare: Callable[[], None] | None = None,
) -> str:
    """Replicate a local dataset to the host at ``url``.

    Takes a new snapshot of the selected parts unless ``snapshot`` names an
    existing one, then streams each part the target lacks – resuming an
    interrupted receive first.

    Args:
        url: Base url of the receiving lakehouse (its api or ``zfs serve``).
        pool: Local ZFS pool path of the lakehouse.
        dataset: Dataset name, the same on both hosts.
        buffer: Bytes to read ahead of the network per stream.
        archive: Include the ``archive`` child.
        statements: Include the ``statements`` child.
        snapshot: Existing snapshot to send instead of taking a new one.
        force: See `check_target`; also discards interrupted receives.
        replace: See `check_target`.
        prepare: Called before a new snapshot is taken – to flush the
            dataset's journal into the store the snapshot captures.

    Returns:
        The name of the snapshot the target now has.
    """
    validate_dataset_name(dataset)
    parts = select_parts(archive, statements)
    with _client(url) as client:

        def _snapshot() -> str:
            if prepare is not None:
                prepare()
            return snapshot_dataset(pool, dataset, parts)

        def _abort(part: str) -> None:
            _request(client, "DELETE", f"/{dataset}/_api/zfs/{part}/receive")

        def _transfer(part: str, plan: Transfer) -> None:
            stream = SendStream(
                zfs_dataset(pool, dataset, part),
                buffer,
                snapshot=plan.snapshot,
                since=plan.since,
                token=plan.token,
            )
            params: dict[str, str | bool] = {
                "base": plan.base or "",
                "force": force,
                "replace": replace,
            }
            endpoint = f"/{dataset}/_api/zfs/{part}/receive"
            try:
                with client.stream(
                    "PUT", endpoint, params=params, content=iter(stream)
                ) as response:
                    _raise_for_status(response)
            finally:
                # a server that answers early (a rejected receive) leaves
                # the body half-read: closing it is what ends the local send
                stream.close()

        return _replicate(
            parts,
            lambda: dataset_status(pool, dataset),
            lambda: _remote_status(client, dataset),
            _abort,
            _transfer,
            snapshot or _snapshot,
            force,
            replace,
        )


def pull(
    url: str,
    pool: str,
    dataset: str,
    buffer: int,
    archive: bool = True,
    statements: bool = True,
    snapshot: str | None = None,
    force: bool = False,
    replace: bool = False,
) -> str:
    """Replicate a dataset from the host at ``url`` to the local pool.

    The remote host flushes the dataset's journal and takes a new snapshot
    of the selected parts, unless ``snapshot`` names an existing one.
    Arguments as for ``push``.

    Returns:
        The name of the snapshot the local dataset now has.
    """
    validate_dataset_name(dataset)
    parts = select_parts(archive, statements)
    with _client(url) as client:

        def _snapshot() -> str:
            res = _request(
                client,
                "POST",
                f"/{dataset}/_api/zfs/snapshot",
                params={"archive": archive, "statements": statements},
            )
            return str(res["snapshot"])

        def _abort(part: str) -> None:
            abort_receive(pool, dataset, part)

        def _transfer(part: str, plan: Transfer) -> None:
            # the local target may have moved on since it was planned
            target = check_receive(pool, dataset, part, plan.base, force, replace)
            params = {
                key: value
                for key, value in (
                    ("snapshot", plan.snapshot),
                    ("since", plan.since),
                    ("token", plan.token),
                )
                if value
            }
            endpoint = f"/{dataset}/_api/zfs/{part}/send"
            with client.stream("GET", endpoint, params=params) as response:
                _raise_for_status(response)
                run_receive(
                    target,
                    response.iter_raw(),
                    buffer,
                    props=PART_PROPS[part],
                    force=True,
                    resumable=True,
                )

        return _replicate(
            parts,
            lambda: _remote_status(client, dataset),
            lambda: dataset_status(pool, dataset),
            _abort,
            _transfer,
            snapshot or _snapshot,
            force,
            replace,
        )
