"""ZFS CLI commands: manual dataset creation.

Sub-typer group:

    ftm-lakehouse zfs init <ds>  # create tuned ZFS datasets for one dataset

The host-side socket agent is the external ``zfs-agent`` package – run it
with its own ``zfs-agent`` command (configured via ``ZFS_SOCKET`` /
``ZFS_POOL`` / ``ZFS_OWNER`` / ``ZFS_ALLOWED_UID``).
"""

from typing import Annotated, Optional

import typer
from anystore.logging import get_logger

from ftm_lakehouse.cli import cli, console, settings
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs import ensure_zfs_dataset

log = get_logger(__name__)

zfs = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(zfs, name="zfs", help="ZFS dataset management for the lakehouse")


@zfs.command("init")
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
            "[red]No ZFS pool specified. Use --pool or set LAKEHOUSE_ZFS_POOL.[/red]"
        )
        raise typer.Exit(code=1)
    ensure_zfs_dataset(zfs_pool, dataset)
    log.info("ZFS datasets initialized", pool=zfs_pool, dataset=dataset)
