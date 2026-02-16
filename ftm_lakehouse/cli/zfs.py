"""ZFS CLI commands: socket agent and manual dataset creation."""

import os
import signal
import socket
import sys
from typing import Annotated, Optional

import typer
from anystore.logging import get_logger

from ftm_lakehouse.cli import cli, console
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs.agent import handle_connection
from ftm_lakehouse.core.zfs.helpers import ensure_zfs_dataset

log = get_logger(__name__)


@cli.command("zfs-agent")
def cli_zfs_agent(
    socket_path: Annotated[
        Optional[str],
        typer.Option("--socket", "-s", help="Unix socket path to listen on"),
    ] = None,
    pool: Annotated[
        Optional[str],
        typer.Option(
            "--pool",
            "-p",
            help="ZFS pool path (or set LAKEHOUSE_ZFS_POOL)",
        ),
    ] = None,
):
    """Start a ZFS socket agent for container-based deployments.

    Listens on a Unix socket and executes ``zfs create`` commands on behalf
    of containerized clients that lack local ZFS tools.
    """
    settings = Settings()
    sock_path = socket_path or settings.zfs_socket
    if not sock_path:
        console.print(
            "[red]No socket path specified. "
            "Use --socket or set LAKEHOUSE_ZFS_SOCKET.[/red]"
        )
        raise typer.Exit(code=1)

    zfs_pool = pool or settings.zfs_pool
    if not zfs_pool:
        console.print(
            "[red]No pool specified. " "Use --pool or set LAKEHOUSE_ZFS_POOL.[/red]"
        )
        raise typer.Exit(code=1)

    if os.path.exists(sock_path):
        os.unlink(sock_path)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    os.chmod(sock_path, 0o666)
    server.listen(5)

    log.info("zfs-agent listening", socket=sock_path, pool=zfs_pool)

    def _shutdown(_signum, _frame):
        log.info("Shutting down zfs-agent")
        server.close()
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            conn, _ = server.accept()
            handle_connection(conn, zfs_pool)
    finally:
        server.close()
        if os.path.exists(sock_path):
            os.unlink(sock_path)


@cli.command("zfs-init")
def cli_zfs_init(
    dataset: Annotated[str, typer.Argument(help="Dataset name to initialize")],
    pool: Annotated[
        Optional[str],
        typer.Option("--pool", "-p", help="ZFS pool path (or set LAKEHOUSE_ZFS_POOL)"),
    ] = None,
):
    """Create ZFS datasets for a lakehouse dataset.

    Creates the parent, archive, and statements ZFS datasets with
    tuned properties under the given pool.
    """
    settings = Settings()
    zfs_pool = pool or settings.zfs_pool
    if not zfs_pool:
        console.print(
            "[red]No ZFS pool specified. " "Use --pool or set LAKEHOUSE_ZFS_POOL.[/red]"
        )
        raise typer.Exit(code=1)
    ensure_zfs_dataset(zfs_pool, dataset)
    log.info("ZFS datasets initialized", pool=zfs_pool, dataset=dataset)
