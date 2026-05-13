"""Command-line interface for ftm-lakehouse.

Defines the main Typer application, shared state, context managers, and
catalog-level commands (``ls``, ``datasets``).  Submodules register their
own commands by importing the ``cli`` app object from this package.
"""

from typing import Annotated, Optional, TypedDict

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write, smart_write_models
from anystore.logging import configure_logging
from anystore.util import dump_json_model
from pydantic import BaseModel
from rich.console import Console

from ftm_lakehouse import __version__
from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_dataset, get_lakehouse

settings = Settings()
cli = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=settings.debug,
    name="FollowTheMoney Data Lakehouse",
)
console = Console(stderr=True)


class State(TypedDict):
    catalog: Catalog | None
    dataset: Dataset | None


STATE: State = {"catalog": None, "dataset": None}


def write_obj(obj: BaseModel | None, out: str) -> None:
    if out == "-":
        console.print(obj)
    else:
        if obj is not None:
            smart_write(out, dump_json_model(obj, clean=True, newline=True))


class CatalogContext(ErrorHandler):
    def __enter__(self) -> Catalog:
        if not STATE["catalog"]:
            STATE["catalog"] = get_lakehouse()
        catalog = STATE["catalog"]
        assert catalog is not None
        return catalog


class DatasetContext(ErrorHandler):
    def __enter__(self) -> Dataset:
        super().__enter__()
        if not STATE["dataset"]:
            e = RuntimeError("Specify dataset name with `-d` option!")
            if settings.debug:
                raise e
            console.print(f"[red][bold]{e.__class__.__name__}[/bold]: {e}[/red]")
            raise typer.Exit(code=1)
        try:
            STATE["dataset"].ensure()
        except Exception as e:
            if settings.debug:
                raise
            console.print(f"[red][bold]{type(e).__name__}[/bold]: {e}[/red]")
            raise typer.Exit(code=1)
        return STATE["dataset"]


# Sub-typer group names whose commands don't need a catalog set up. The
# top-level callback dispatches on the group name (``ctx.invoked_subcommand``).
SKIP_CATALOG_COMMANDS = {"zfs"}


@cli.callback(invoke_without_command=True)
def cli_ftm_lakehouse(
    ctx: typer.Context,
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False,
    settings: Annotated[
        Optional[bool], typer.Option(..., help="Show current settings")
    ] = False,
    uri: Annotated[str | None, typer.Option(..., help="Lakehouse uri (path)")] = None,
    dataset: Annotated[
        str | None, typer.Option("-d", help="Dataset name (also known as foreign_id)")
    ] = None,
    # dataset_uri: Annotated[
    #     str | None, typer.Option(..., help="Dataset lakehouse uri")
    # ] = None,
):
    if version:
        console.print(__version__)
        raise typer.Exit()
    settings_ = Settings()
    configure_logging(level=settings_.log_level)
    if ctx.invoked_subcommand in SKIP_CATALOG_COMMANDS:
        return
    try:
        catalog = get_lakehouse(uri)
        STATE["catalog"] = catalog
        if dataset:
            # if dataset_uri:
            #     STATE["dataset"] = get_dataset(dataset, dataset_uri)
            # else:
            STATE["dataset"] = get_dataset(dataset)
    except Exception as e:
        if settings_.debug:
            raise
        console.print(f"[red][bold]{type(e).__name__}[/bold]: {e}[/red]")
        raise typer.Exit(code=1)
    if settings:
        console.print(settings_)
        console.print(STATE)
        raise typer.Exit()


@cli.command("ls")
def cli_dataset_names(out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """Show dataset names in the current catalog."""
    with CatalogContext() as catalog:
        names = [d.name for d in catalog.list_datasets()]
        smart_write(out_uri, "\n".join(names) + "\n", "wb")


@cli.command("datasets")
def cli_datasets(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Show metadata for all datasets in the current catalog."""
    with CatalogContext() as catalog:
        datasets = [d.model for d in catalog.list_datasets()]
        smart_write_models(out_uri, datasets)


# Import submodules so their sub-typers and commands get registered on `cli`.
from ftm_lakehouse.cli import (  # noqa: E402, F401
    archive,
    entities,
    mappings,
    operations,
    statements,
    zfs,
)
