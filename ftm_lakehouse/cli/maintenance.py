"""Dataset maintenance commands for the CLI.

``make`` and ``export`` stay at the top level as frequently-used shortcuts:

    ftm-lakehouse -d <dataset> make
    ftm-lakehouse -d <dataset> export <kind>

Everything else groups under ``maintenance``:

    ftm-lakehouse maintenance optimize
    ftm-lakehouse maintenance unlock
"""

from typing import Annotated, Optional

import typer

from ftm_lakehouse import operation as op
from ftm_lakehouse.catalog import get_dataset_index
from ftm_lakehouse.cli import (
    OPT_FORCE,
    DatasetContext,
    cli,
    console,
    sub_typer,
    write_config,
)
from ftm_lakehouse.operation.export import ExportKind
from ftm_lakehouse.repository.factories import get_entities

maintenance = sub_typer("maintenance", "Dataset maintenance operations")


# ---------------------------------------------------------------------------
# Top-level shortcut: ``make``
# ---------------------------------------------------------------------------


@cli.command("make")
def cli_make(
    config: Annotated[
        Optional[str],
        typer.Option("-c", help="Configuration yml to store as `config.yml`"),
    ] = None,
    flush: Annotated[
        Optional[bool],
        typer.Option(help="Flush outstanding journal statements to store"),
    ] = True,
    exports: Annotated[
        Optional[bool],
        typer.Option(
            help="Include export statements/entities and diffs, compute stats"
        ),
    ] = True,
    optimize: Annotated[
        Optional[bool],
        typer.Option(help="Optimize parquet store beforehand when using --exports"),
    ] = True,
    force_optimize: Annotated[
        Optional[bool],
        typer.Option(help="Re-optimize even if up-to-date."),
    ] = False,
    force_exports: Annotated[
        Optional[bool],
        typer.Option(help="Re-compute full exports pipeline even if up-to-date."),
    ] = False,
):
    """Make or update a dataset.

    By default this flushes the journal, optimizes the parquet store and
    regenerates all exports. Use ``--no-exports`` to only flush and refresh
    ``index.json``, or ``--no-optimize`` to export without the maintenance pass.
    """
    with DatasetContext() as (name, uri):
        if config:
            write_config(name, uri, config)
        if flush:
            get_entities(name, uri).flush()
        if exports:
            if optimize:
                op.optimize(name, uri, force=bool(force_optimize))
            op.make(name, uri, force=bool(force_exports))
        else:
            op.export(name, ExportKind.index, uri, force=bool(force_exports))
        console.print(get_dataset_index(name, uri))


# ---------------------------------------------------------------------------
# Exports (top level cli)
# ---------------------------------------------------------------------------


@cli.command("export")
def cli_export(
    kind: Annotated[ExportKind, typer.Argument(help="Which export to produce.")],
    force: OPT_FORCE = False,
):
    """Export the dataset: ``statements`` (statements.csv), ``entities``
    (entities.ftm.json), ``documents`` (documents.csv), ``statistics``
    (statistics.json) or ``index`` (index.json)."""
    with DatasetContext() as (name, uri):
        res = op.export(name, kind, uri, force=bool(force))
        console.print(res)


# ---------------------------------------------------------------------------
# Async maintenance on the parquet statement store
# ---------------------------------------------------------------------------


@maintenance.command("optimize")
def cli_optimize(
    retention_hours: Annotated[
        Optional[int],
        typer.Option(help="Vacuum: retain obsolete files newer than this many hours."),
    ] = 0,
    force: OPT_FORCE = False,
):
    """Optimize the statement store: collapse duplicates and reap expired
    tombstones, bin-pack small parquet files, delete obsolete files.

    Tombstones older than ``LAKEHOUSE_GRACE_PERIOD_DAYS`` are dropped. Each
    step is held under the dataset write fence.
    """
    with DatasetContext() as (name, uri):
        res = op.optimize(
            name, uri, retention_hours=int(retention_hours or 0), force=bool(force)
        )
        console.print(res)


@maintenance.command("unlock")
def cli_unlock():
    """Forcibly release the dataset write fence.

    Use when a previous writer (flush / merge / compact / vacuum / append)
    died with the lock held and subsequent writes hang trying to acquire
    it. The lock is just a file at ``<dataset>/.LOCK``.

    **Confirm no process is actively writing** before running – breaking
    a held lock can corrupt an in-flight write. No-op if no lock is held.

    Local-only: the lock is a storage-side file – run this where the
    storage is directly accessible.
    """
    with DatasetContext() as (name, uri):
        entities = get_entities(name, uri)
        if entities._is_api:
            raise RuntimeError("`maintenance unlock` is not available in API mode")
        if entities.unlock():
            console.print("[green]Lock released.[/green]")
        else:
            console.print("[yellow]No lock held.[/yellow]")
