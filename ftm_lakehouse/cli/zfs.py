"""ZFS socket agent CLI command."""

import os
import signal
import socket
import sys
from typing import Annotated, Optional

import typer
from anystore.logging import configure_logging, get_logger

from ftm_lakehouse.cli import cli, console
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs.agent import handle_connection

log = get_logger(__name__)

zfs = typer.Typer(
    no_args_is_help=True,
    invoke_without_command=True,
)
cli.add_typer(zfs, name="zfs-agent", help="ZFS socket agent for container deployments")


@zfs.callback(invoke_without_command=True)
def cli_zfs_agent(
    socket_path: Annotated[
        Optional[str],
        typer.Option("--socket", "-s", help="Unix socket path to listen on"),
    ] = None,
    prefix: Annotated[
        Optional[str],
        typer.Option(
            "--prefix",
            "-p",
            help="Required ZFS dataset prefix for validation (e.g. 'tank/lakehouse')",
        ),
    ] = None,
):
    """Start a ZFS socket agent for container-based deployments.

    Listens on a Unix socket and executes ``zfs create`` commands on behalf
    of containerized clients that lack local ZFS tools.
    """
    settings = Settings()
    configure_logging(level=settings.log_level)

    sock_path = socket_path or settings.zfs_socket
    if not sock_path:
        console.print(
            "[red]No socket path specified. "
            "Use --socket or set LAKEHOUSE_ZFS_SOCKET.[/red]"
        )
        raise typer.Exit(code=1)

    if os.path.exists(sock_path):
        os.unlink(sock_path)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(5)

    def _shutdown(_signum, _frame):
        log.info("Shutting down zfs-agent")
        server.close()
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("zfs-agent listening", socket=sock_path, prefix=prefix)

    try:
        while True:
            conn, _ = server.accept()
            handle_connection(conn, prefix)
    finally:
        server.close()
        if os.path.exists(sock_path):
            os.unlink(sock_path)
