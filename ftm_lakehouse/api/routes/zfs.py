"""ZFS replication routes: snapshot status, snapshots, send / receive streams.

Mounted into the api when ``LAKEHOUSE_ZFS_API`` is set, and served on their
own by ``ftm-lakehouse zfs serve``; the client side is ``core.zfs.push`` /
``pull``. A failing ``zfs`` – or a receive the target refuses – answers 409
with the reason as ``detail``; an unreachable ``zfs-agent`` answers 503.
"""

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from ftm_lakehouse.api.dependencies import DatasetName
from ftm_lakehouse.catalog import dataset_exists
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs import (
    PART_PROPS,
    DatasetStatus,
    SendStream,
    abort_receive,
    arun_receive,
    check_receive,
    dataset_status,
    select_parts,
    snapshot_dataset,
    zfs_dataset,
)
from ftm_lakehouse.repository.factories import clear_caches, get_entities

router = APIRouter()


def get_zfs_pool(request: Request) -> str:
    """The ZFS pool the serving app was built for."""
    pool: str = request.app.state.zfs_pool
    return pool


ZfsPool = Annotated[str, Depends(get_zfs_pool)]


def _buffer() -> int:
    return int(Settings().zfs_buffer)


@contextmanager
def _zfs_errors() -> Iterator[None]:
    try:
        yield
    except (RuntimeError, ValueError) as e:
        raise HTTPException(409, detail=str(e))
    except OSError as e:
        raise HTTPException(503, detail=f"zfs-agent unavailable: {e}")


class SendResponse(StreamingResponse):
    """Stops the send once the response is over, however it ended – a
    client gone mid-download never gets the body iterator closed."""

    def __init__(self, stream: SendStream, first: bytes) -> None:
        self.stream = stream

        async def body() -> AsyncIterator[bytes]:
            yield first
            async for chunk in stream:
                yield chunk

        super().__init__(body(), media_type="application/octet-stream")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.stream.close()


@router.get("/{dataset}/_api/zfs")
def zfs_status_route(dataset: DatasetName, pool: ZfsPool) -> DatasetStatus:
    """Snapshots (oldest first, with guids) and pending resume token of the
    dataset's ``base`` / ``archive`` / ``statements`` ZFS datasets."""
    with _zfs_errors():
        return dataset_status(pool, dataset)


@router.post("/{dataset}/_api/zfs/snapshot")
def zfs_snapshot_route(
    dataset: DatasetName,
    pool: ZfsPool,
    request: Request,
    archive: bool = True,
    statements: bool = True,
) -> dict[str, str]:
    """Flush the dataset's journal, then snapshot ``base`` and the selected
    children atomically – what a pull then sends."""
    uri = request.app.state.lake.dataset_uri(dataset)
    if dataset_exists(dataset, uri):
        get_entities(dataset, uri).flush()
    with _zfs_errors():
        name = snapshot_dataset(pool, dataset, select_parts(archive, statements))
    return {"snapshot": name}


@router.get("/{dataset}/_api/zfs/{part}/send")
def zfs_send_route(
    dataset: DatasetName,
    part: str,
    pool: ZfsPool,
    snapshot: str | None = None,
    since: str | None = None,
    token: str | None = None,
) -> SendResponse:
    """Stream ``zfs send`` of ``part@snapshot`` (incremental from ``since``),
    or resume an interrupted receive with its ``token`` – which has to be one
    for this part, ``zfs-agent`` refuses any other.

    Errors before the first byte answer 409. Past that the status is sent,
    so a failure cuts the stream short – which the receiving ``zfs receive``
    rejects, keeping the partial state for a resume.
    """
    if not snapshot and not token:
        raise ValueError("Either `snapshot` or `token` is required")
    stream = SendStream(
        zfs_dataset(pool, dataset, part),
        _buffer(),
        snapshot=snapshot,
        since=since,
        token=token,
    )
    with _zfs_errors():
        first = stream.first()
    return SendResponse(stream, first)


@router.put("/{dataset}/_api/zfs/{part}/receive")
async def zfs_receive_route(
    dataset: DatasetName,
    part: str,
    pool: ZfsPool,
    request: Request,
    base: str | None = None,
    force: bool = False,
    replace: bool = False,
) -> dict[str, bool]:
    """Receive the request body as a ``zfs send`` stream into ``part``.

    ``base`` is the guid of the snapshot the stream applies on top of (empty
    for a full stream). It is checked against the target (``check_receive``)
    before a byte of the body is read – that the client planned the same is
    no guarantee, the target may have moved on since. Then ``zfs receive -s
    -F`` runs with the part's tuning, and the answer comes once it has
    finished. The worker's repository caches are cleared after: the
    received ``config.yml`` may have changed layout (``shards``).
    """
    zfs_dataset(pool, dataset, part)  # an invalid part is a 400, not a 409
    with _zfs_errors():
        target = await anyio.to_thread.run_sync(
            check_receive, pool, dataset, part, base or None, force, replace
        )
        await arun_receive(
            target,
            request.stream(),
            _buffer(),
            props=PART_PROPS[part],
            force=True,
            resumable=True,
        )
    clear_caches()
    return {"ok": True}


@router.delete("/{dataset}/_api/zfs/{part}/receive")
def zfs_abort_route(dataset: DatasetName, part: str, pool: ZfsPool) -> dict[str, bool]:
    """Discard the partial state of an interrupted receive into ``part``."""
    zfs_dataset(pool, dataset, part)
    with _zfs_errors():
        abort_receive(pool, dataset, part)
    return {"ok": True}
