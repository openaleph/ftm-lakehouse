"""Dataset operation commands for the CLI.

``make`` stays at the top level as a frequently-used shortcut:

    ftm-lakehouse make --full

Everything else groups under ``operations``:

    ftm-lakehouse operations export <kind>
    ftm-lakehouse operations optimize
    ftm-lakehouse operations unlock
    ftm-lakehouse operations crawl <uri>
"""

from typing import Annotated, Optional

import typer

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import DatasetContext, cli, console, settings, write_obj
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.operation.crawl import HandleExistingMode
from ftm_lakehouse.operation.export import ExportKind

operations = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(operations, name="operations", help="Dataset pipeline operations")


# ---------------------------------------------------------------------------
# Top-level shortcut: ``make``
# ---------------------------------------------------------------------------


@cli.command("make")
def cli_make(
    config: Annotated[
        Optional[str],
        typer.Option("-c", help="Configuration yml to store as `config.yml`"),
    ] = None,
    full: Annotated[
        Optional[bool],
        typer.Option(
            help="Run full update: flush journal, export statements/entities, compute stats"
        ),
    ] = False,
    force: Annotated[
        Optional[bool],
        typer.Option(help="Re-compute full exports pipeline even if up-to-date."),
    ] = False,
):
    """Make or update a dataset.

    Use ``--full`` for a complete update including flushing the journal and
    generating all exports.
    """
    with DatasetContext() as dataset:
        if config:
            dataset_config = DatasetModel.from_yaml_uri(config)
            dataset.update_model(**dataset_config.model_dump())
        if full:
            op.make(dataset, force=bool(force))
        else:
            dataset.get_entities().flush()
            op.export(dataset, ExportKind.index, force=bool(force))
        console.print(dataset.index)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------


@operations.command("export")
def cli_export(
    kind: Annotated[ExportKind, typer.Argument(help="Which export to produce.")],
    force: Annotated[
        Optional[bool], typer.Option(help="Run regardless of freshness state.")
    ] = False,
):
    """Export the dataset: ``statements`` (statements.csv), ``entities``
    (entities.ftm.json), ``documents`` (documents.csv), ``statistics``
    (statistics.json) or ``index`` (index.json)."""
    with DatasetContext() as dataset:
        res = op.export(dataset, kind, force=bool(force))
        console.print(res)


# ---------------------------------------------------------------------------
# Async maintenance on the parquet statement store
# ---------------------------------------------------------------------------


@operations.command("optimize")
def cli_optimize(
    retention_hours: Annotated[
        Optional[int],
        typer.Option(help="Vacuum: retain obsolete files newer than this many hours."),
    ] = 0,
    force: Annotated[
        Optional[bool], typer.Option(help="Run regardless of freshness state.")
    ] = False,
):
    """Optimize the statement store: collapse duplicates and reap expired
    tombstones, bin-pack small parquet files, delete obsolete files.

    Tombstones older than ``LAKEHOUSE_GRACE_PERIOD_DAYS`` are dropped. Each
    step is held under the dataset write fence.
    """
    with DatasetContext() as dataset:
        res = op.optimize(
            dataset, retention_hours=int(retention_hours or 0), force=bool(force)
        )
        console.print(res)


@operations.command("unlock")
def cli_unlock():
    """Forcibly release the dataset write fence.

    Use when a previous writer (flush / merge / compact / vacuum / append)
    died with the lock held and subsequent writes hang trying to acquire
    it. The lock is just a file at ``<dataset>/.LOCK``.

    **Confirm no process is actively writing** before running – breaking
    a held lock can corrupt an in-flight write. No-op if no lock is held.
    """
    with DatasetContext() as dataset:
        if dataset.get_entities().unlock():
            console.print("[green]Lock released.[/green]")
        else:
            console.print("[yellow]No lock held.[/yellow]")


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------


@operations.command("crawl")
def cli_crawl(
    uri: str,
    out_uri: Annotated[
        str, typer.Option("-o", help="Write results to this destination")
    ] = "-",
    exclude: Annotated[
        Optional[str], typer.Option(help="Exclude paths glob pattern")
    ] = None,
    include: Annotated[
        Optional[str], typer.Option(help="Include paths glob pattern")
    ] = None,
    make_entities: Annotated[
        Optional[bool], typer.Option(help="Create entities from crawled files")
    ] = True,
    existing: Annotated[
        Optional[HandleExistingMode], typer.Option(help="How to handle existing files")
    ] = HandleExistingMode.overwrite,
):
    """Crawl documents from local or remote sources into the archive."""
    with DatasetContext() as dataset:
        result = op.crawl(
            dataset,
            uri,
            glob=include,
            exclude_glob=exclude,
            make_entities=make_entities,
            existing=existing,
        )
        write_obj(result, out_uri)
