"""Dataset maintenance commands for the CLI.

``make`` and ``export`` stay at the top level as frequently-used shortcuts:

    ftm-lakehouse -d <dataset> make
    ftm-lakehouse -d <dataset> export <kind>

Everything else groups under ``maintenance``:

    ftm-lakehouse maintenance flush [--all]
    ftm-lakehouse maintenance optimize
    ftm-lakehouse maintenance shard --shards <n>
    ftm-lakehouse maintenance migrate [--all]
    ftm-lakehouse maintenance unlock
"""

from typing import Annotated, Optional

import typer

from ftm_lakehouse import operation as op
from ftm_lakehouse.catalog import get_dataset_index
from ftm_lakehouse.cli import (
    OPT_FORCE,
    STATE,
    CatalogContext,
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
# Journal drain
# ---------------------------------------------------------------------------


@maintenance.command("flush")
def cli_flush(
    all_: Annotated[
        bool,
        typer.Option(
            "--all", help="Sweep the whole catalog (not combinable with `-d`)"
        ),
    ] = False,
):
    """Drain outstanding journal statements into the parquet store.

    With ``--all`` every dataset in the catalog is swept in turn. It addresses
    the whole catalog, so it is mutually exclusive with ``-d``.

    Duplicates and tombstones land as new rows – run ``maintenance optimize``
    afterwards to collapse them. In api mode the flush is delegated to the
    server.
    """
    if all_:
        if STATE["dataset"]:
            console.print("[red]Use either `-d <dataset>` or `--all`, not both.[/red]")
            raise typer.Exit(code=1)
        with CatalogContext() as catalog:
            total = 0
            for name in catalog.list_datasets():
                count = get_entities(name, catalog.dataset_uri(name)).flush()
                total += count
            console.print(f"[green]Flushed {total} statements in total.[/green]")
    else:
        with DatasetContext() as (name, uri):
            total = get_entities(name, uri).flush()
            console.print(f"[green]Flushed {total} statements.[/green]")


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


@maintenance.command("shard")
def cli_shard(
    shards: Annotated[
        int,
        typer.Option(help="Target number of entity-id hash shards (0/1 = single)."),
    ],
    force: OPT_FORCE = False,
) -> None:
    """Re-shard the statement store: rewrite it onto a new shard count and
    record that count in ``config.yml``.

    The shard count is otherwise fixed at dataset creation, since every
    reader and writer resolves it from the config. Growing it is the fix for
    a dataset whose partitions have become too big to query well; see
    ``docs/architecture.md`` for how to size it.

    A full rewrite of the store: the journal is drained first, every
    ``(bucket, origin)`` group is streamed into its new shard partitions,
    and the config is written last. Nothing is deduped or sorted on the way,
    so follow up with ``maintenance optimize``.

    **Run with writers stopped** – the write fence holds off parquet
    appends, but statements journalled under the old count and flushed
    afterwards land in the wrong partition.
    """
    with DatasetContext() as (name, uri):
        res = op.shard(name, int(shards), uri, force=bool(force))
        console.print(res)


@maintenance.command("migrate")
def cli_migrate(
    all_: Annotated[
        bool,
        typer.Option(
            "--all", help="Sweep the whole catalog (not combinable with `-d`)"
        ),
    ] = False,
    force: OPT_FORCE = False,
) -> None:
    """Apply the storage-layout migrations a dataset hasn't seen yet.

    Migrations bring a store written by an older version up to the layout the
    current code reads. Each is stamped when it completes, so this is a no-op
    on an up-to-date dataset and a half-finished run resumes where it stopped.

    With ``--all`` every dataset in the catalog is swept in turn – how the
    docker entrypoint runs it. Run with writers stopped: a migration takes the
    exclusive write fence.
    """
    if all_:
        if STATE["dataset"]:
            console.print("[red]Use either `-d <dataset>` or `--all`, not both.[/red]")
            raise typer.Exit(code=1)
        with CatalogContext() as catalog:
            for name in catalog.list_datasets():
                res = op.migrate(name, catalog.dataset_uri(name), force=bool(force))
                console.print(res)
    else:
        with DatasetContext() as (name, uri):
            res = op.migrate(name, uri, force=bool(force))
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
