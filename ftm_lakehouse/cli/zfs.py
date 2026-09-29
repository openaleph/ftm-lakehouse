"""ZFS CLI commands: dataset creation and replication.

Sub-typer group:

    ftm-lakehouse -d <ds> zfs init             # create tuned ZFS datasets
    ftm-lakehouse zfs serve                    # replication api on its own
    ftm-lakehouse -d <ds> zfs status [URL]     # local (and remote) snapshots
    ftm-lakehouse -d <ds> zfs push URL         # replicate to another host
    ftm-lakehouse -d <ds> zfs pull URL         # replicate from another host

The host-side socket agent is the external ``zfs-agent`` package – run it
with its own ``zfs-agent`` command (configured via ``ZFS_SOCKET`` /
``ZFS_POOL`` / ``ZFS_OWNER`` / ``ZFS_ALLOWED_UID`` / ``ZFS_ACTIONS``).
"""

import json
import os
from types import TracebackType
from typing import Annotated, NamedTuple, NoReturn, Optional

import typer
from anystore.logging import get_logger
from pydantic import TypeAdapter

from ftm_lakehouse.catalog import dataset_exists
from ftm_lakehouse.cli import STATE, console, settings, sub_typer
from ftm_lakehouse.core import zfs as zfs_ops
from ftm_lakehouse.core.settings import Settings, ZfsBuffer
from ftm_lakehouse.core.zfs import ensure_zfs_dataset
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.repository.factories import get_entities

try:  # optional `api` extra – only `zfs serve` needs it
    from granian import Granian
    from granian.constants import Interfaces

    HAS_GRANIAN = True
except ImportError:  # pragma: no cover
    HAS_GRANIAN = False

log = get_logger(__name__)

zfs = sub_typer("zfs", "ZFS dataset management for the lakehouse")

OPT_POOL = Annotated[
    Optional[str],
    typer.Option("--pool", "-p", help="ZFS pool path (or set LAKEHOUSE_ZFS_POOL)"),
]
OPT_URL = Annotated[
    str,
    typer.Argument(help="Base url of the other lakehouse (its api or `zfs serve`)"),
]
OPT_ARCHIVE = Annotated[bool, typer.Option(help="Include the `archive` child dataset")]
OPT_STATEMENTS = Annotated[
    bool, typer.Option(help="Include the `statements` child dataset")
]
OPT_SNAPSHOT = Annotated[
    Optional[str],
    typer.Option(help="Send this existing snapshot instead of taking a new one"),
]
OPT_ZFS_FORCE = Annotated[
    bool,
    typer.Option(
        "--force",
        help="Let the receive destroy target snapshots newer than the common "
        "base, and discard interrupted receives instead of resuming them",
    ),
]
OPT_REPLACE = Annotated[
    bool,
    typer.Option(
        "--replace",
        help="Let a full receive replace a target that exists without snapshots",
    ),
]
OPT_BUFFER = Annotated[
    Optional[str],
    typer.Option(
        help="Stream buffer per transfer, e.g. `2GiB` (or set LAKEHOUSE_ZFS_BUFFER)"
    ),
]


def _resolve_pool(pool: str | None) -> str:
    zfs_pool = pool or Settings().zfs_pool
    if not zfs_pool:
        console.print(
            "[red]No ZFS pool specified. Use --pool or set LAKEHOUSE_ZFS_POOL.[/red]"
        )
        raise typer.Exit(code=1)
    return zfs_pool


class ZfsRef(NamedTuple):
    dataset: str
    pool: str


class ZfsContext:
    """Yield the `ZfsRef` of the dataset addressed via ``-d``.

    The ``zfs`` group's ``DatasetContext``: the same ``-d`` check and
    one-line red errors (re-raised with ``DEBUG``), for the body too – but
    no catalog and no ``ensure_dataset``. That writes ``config.yml`` and,
    with ``LAKEHOUSE_ON_ZFS``, creates the ZFS datasets, which a ``pull``
    has to find absent so its full receive can create them:

        with ZfsContext(pool) as (dataset, zfs_pool):
            zfs_ops.pull(url, zfs_pool, dataset, ...)
    """

    def __init__(self, pool: str | None = None) -> None:
        self.pool = pool

    def __enter__(self) -> ZfsRef:
        dataset = STATE["dataset"]
        if not dataset:
            self._fail(RuntimeError("Specify dataset name with `-d` option!"))
        return ZfsRef(dataset, _resolve_pool(self.pool))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if isinstance(exc, Exception) and not isinstance(exc, typer.Exit):
            self._fail(exc)

    @staticmethod
    def _fail(exc: BaseException) -> NoReturn:
        if settings.debug:
            raise exc
        console.print(f"[red][bold]{type(exc).__name__}[/bold]: {exc}[/red]")
        raise typer.Exit(code=1)


def _buffer(buffer: str | None) -> int:
    if buffer is None:
        return int(Settings().zfs_buffer)
    return int(TypeAdapter(ZfsBuffer).validate_python(buffer))


def _flush(dataset: str) -> None:
    """Drain the dataset's journal into its store, so a snapshot has it."""
    uri = get_lakehouse().dataset_uri(dataset)
    if not dataset_exists(dataset, uri):
        log.warning(
            "Dataset not in the catalog (LAKEHOUSE_URI) – journal not flushed",
            dataset=dataset,
        )
        return
    get_entities(dataset, uri).flush()


@zfs.command("init")
def cli_zfs_init(pool: OPT_POOL = None) -> None:
    """Create ZFS datasets for a lakehouse dataset.

    Creates the parent, archive, and statements ZFS datasets with
    tuned properties under the given pool.
    """
    with ZfsContext(pool) as (dataset, zfs_pool):
        ensure_zfs_dataset(zfs_pool, dataset)
    log.info("ZFS datasets initialized", pool=zfs_pool, dataset=dataset)


@zfs.command("serve")
def cli_zfs_serve(
    host: Annotated[
        str, typer.Option(help="Interface to bind – no auth, keep it internal")
    ] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to listen on")] = 8881,
    pool: OPT_POOL = None,
) -> None:
    """Serve the ZFS replication api (`/{dataset}/_api/zfs/...`) on its own.

    For hosts whose lakehouse api isn't exposed – otherwise set
    `LAKEHOUSE_ZFS_API=1` on the api instead. There is no authentication:
    whoever reaches the port can read and overwrite every dataset in the pool.
    """
    if not HAS_GRANIAN:
        console.print("[red]`zfs serve` needs the `api` extra (granian).[/red]")
        raise typer.Exit(code=1)
    # workers build the app from the environment
    os.environ["LAKEHOUSE_ZFS_POOL"] = _resolve_pool(pool)
    Granian(
        "ftm_lakehouse.api:zfs_app",
        address=host,
        port=port,
        interface=Interfaces.ASGI,
    ).serve()


@zfs.command("status")
def cli_zfs_status(
    url: Annotated[
        Optional[str], typer.Argument(help="Also show this host's status")
    ] = None,
    pool: OPT_POOL = None,
) -> None:
    """Show snapshots (with guids) and resume tokens of the dataset's parts."""
    with ZfsContext(pool) as (dataset, zfs_pool):
        status = {"local": zfs_ops.dataset_status(zfs_pool, dataset)}
        if url:
            status["remote"] = zfs_ops.remote_status(url, dataset)
    typer.echo(json.dumps(status, indent=2))


@zfs.command("push")
def cli_zfs_push(
    url: OPT_URL,
    pool: OPT_POOL = None,
    archive: OPT_ARCHIVE = True,
    statements: OPT_STATEMENTS = True,
    snapshot: OPT_SNAPSHOT = None,
    force: OPT_ZFS_FORCE = False,
    replace: OPT_REPLACE = False,
    buffer: OPT_BUFFER = None,
) -> None:
    """Replicate the dataset to another host.

    Flushes the dataset's journal and takes a new snapshot unless
    `--snapshot` is given, then sends each part the other host lacks –
    incremental from the newest snapshot both share, resuming an interrupted
    receive first. Every part is checked before anything is sent.
    """
    with ZfsContext(pool) as (dataset, zfs_pool):
        name = zfs_ops.push(
            url,
            zfs_pool,
            dataset,
            _buffer(buffer),
            archive=archive,
            statements=statements,
            snapshot=snapshot,
            force=force,
            replace=replace,
            prepare=lambda: _flush(dataset),
        )
    log.info("Pushed", dataset=dataset, snapshot=name, url=url)


@zfs.command("pull")
def cli_zfs_pull(
    url: OPT_URL,
    pool: OPT_POOL = None,
    archive: OPT_ARCHIVE = True,
    statements: OPT_STATEMENTS = True,
    snapshot: OPT_SNAPSHOT = None,
    force: OPT_ZFS_FORCE = False,
    replace: OPT_REPLACE = False,
    buffer: OPT_BUFFER = None,
) -> None:
    """Replicate the dataset from another host.

    The other host flushes the dataset's journal and takes a new snapshot
    unless `--snapshot` is given; each part the local pool lacks is then
    received as for `push`.
    """
    with ZfsContext(pool) as (dataset, zfs_pool):
        name = zfs_ops.pull(
            url,
            zfs_pool,
            dataset,
            _buffer(buffer),
            archive=archive,
            statements=statements,
            snapshot=snapshot,
            force=force,
            replace=replace,
        )
    log.info("Pulled", dataset=dataset, snapshot=name, url=url)
