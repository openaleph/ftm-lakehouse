"""Command-line interface for ftm-lakehouse.

Defines the main Typer application, shared state, context managers, and
catalog-level commands (``ls``, ``datasets``).  Submodules register their
own commands by importing the ``cli`` app object from this package.
"""

from datetime import datetime
from typing import Annotated, Optional, TypedDict

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write, smart_write_models
from anystore.logging import configure_logging
from anystore.util import dump_json_model
from ftmq.query import Query
from pydantic import BaseModel
from rich.console import Console

from ftm_lakehouse import __version__
from ftm_lakehouse.catalog import (
    Catalog,
    ensure_dataset,
    get_dataset_model,
    update_dataset,
)
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.model.dataset import DatasetModel, get_model_class
from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.base import dataset_uri as repo_dataset_uri

settings = Settings()
cli = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=settings.debug,
    name="FollowTheMoney Data Lakehouse",
)
console = Console(stderr=True)


def sub_typer(name: str, help: str) -> typer.Typer:
    """Create a sub-command group and register it on the main ``cli`` app."""
    app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
    cli.add_typer(app, name=name, help=help)
    return app


# Shared command options – import these instead of redeclaring per command.
OPT_IN = Annotated[str, typer.Option("-i")]
OPT_OUT = Annotated[str, typer.Option("-o")]
OPT_ORIGIN = Annotated[
    str, typer.Option(help="Default data origin if the input carries none")
]
OPT_OVERRIDE_ORIGIN = Annotated[
    bool, typer.Option(help="Force the given origin over input-carried origins")
]
OPT_ROLE = Annotated[
    str | None,
    typer.Option(help="Default role (who asserts) if the input carries none"),
]
OPT_BULK_SIZE = Annotated[
    int, typer.Option(help="Number of statements buffered before flush to parquet.")
]
OPT_LAST_SEEN = Annotated[
    Optional[datetime],
    typer.Option(help="Default last_seen timestamp if the input has none"),
]
OPT_UNSAFE = Annotated[
    bool,
    typer.Option(
        "--unsafe",
        help="Fast path: map input straight to parquet rows, skipping FtM "
        "object construction and validation. Trusted input only.",
    ),
]
OPT_FORCE = Annotated[
    Optional[bool], typer.Option(help="Run regardless of freshness state.")
]
OPT_QUERY = Annotated[
    Optional[str],
    typer.Option(
        "-q",
        "--query",
        help="Filter query string, e.g. "
        "'filter:schema=Person&filter:group.countries=de'",
    ),
]
OPT_RQL = Annotated[
    Optional[str],
    typer.Option(
        "--rql",
        help="RQL query string (nested & | ~), e.g. "
        "'and(eq(schema,Person),or(eq(group.countries,de),eq(group.countries,at)))'",
    ),
]


class State(TypedDict):
    catalog: Catalog | None
    dataset: str | None
    dataset_uri: str | None


STATE: State = {"catalog": None, "dataset": None, "dataset_uri": None}


def write_config(name: str, uri: str, config: str) -> DatasetModel:
    """Merge a configuration yaml into the dataset's ``config.yml``.

    Only keys actually present in the yaml are written, so this is a real
    merge – omitting ``shards`` leaves the configured shard count alone
    instead of resetting it to the default. The dataset is addressed via
    ``-d``, so ``name`` and ``uri`` from the yaml are ignored – the name is
    injected merely to satisfy validation.

    Args:
        name: Dataset name.
        uri: Dataset storage root.
        config: Uri of the configuration yaml to read.

    Returns:
        The updated dataset model.
    """
    model = get_model_class()
    data = model.from_yaml_uri(config, name=name).model_dump(
        exclude={"name", "uri"}, exclude_unset=True
    )
    return update_dataset(name, uri, **data)


def parse_query(query: str | None = None, rql: str | None = None) -> Query | None:
    """Build an ftmq `Query` from a CLI query string.

    ``-q`` takes the flat Aleph filter grammar, ``--rql`` the nested RQL one –
    they are alternatives, not combinable. Either string is a whole query: it
    carries sort and slice as well, not just filters.

    Args:
        query: Aleph-style filter query string.
        rql: RQL query string.

    Returns:
        The parsed ``Query``, or ``None`` if no query string was given.

    Raises:
        ValueError: if both ``query`` and ``rql`` are given.
    """
    if query and rql:
        raise ValueError("Use either `-q` or `--rql`, not both.")
    if query:
        return Query.from_string(query)
    if rql:
        return Query.from_rql(rql)
    return None


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
    """Yield the `DatasetRef` of the dataset addressed via ``-d``.

    Ensures the dataset exists (creates ``config.yml`` if needed) on entry –
    commands resolve repositories themselves via the factories:

        with DatasetContext() as (name, uri):
            repo = get_entities(name, uri)
    """

    def __enter__(self) -> DatasetRef:
        super().__enter__()
        name, uri = STATE["dataset"], STATE["dataset_uri"]
        if not name or not uri:
            e = RuntimeError("Specify dataset name with `-d` option!")
            if settings.debug:
                raise e
            console.print(f"[red][bold]{e.__class__.__name__}[/bold]: {e}[/red]")
            raise typer.Exit(code=1)
        try:
            ensure_dataset(name, uri)
        except Exception as e:
            if settings.debug:
                raise
            console.print(f"[red][bold]{type(e).__name__}[/bold]: {e}[/red]")
            raise typer.Exit(code=1)
        return DatasetRef(name, uri)


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
    dataset_uri: Annotated[
        str | None, typer.Option(..., help="Dataset lakehouse uri")
    ] = None,
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
            STATE["dataset"] = dataset
            # normalize + validate either way; --dataset-uri overrides the
            # catalog-derived location
            STATE["dataset_uri"] = (
                repo_dataset_uri(dataset, dataset_uri)
                if (dataset_uri)
                else catalog.dataset_uri(dataset)
            )
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
def cli_dataset_names(out_uri: OPT_OUT = "-"):
    """Show dataset names in the current catalog."""
    with CatalogContext() as catalog:
        names = list(catalog.list_datasets())
        smart_write(out_uri, "\n".join(names) + "\n", "wb")


@cli.command("datasets")
def cli_datasets(
    out_uri: OPT_OUT = "-",
):
    """Show metadata for all datasets in the current catalog."""
    with CatalogContext() as catalog:
        datasets = [
            get_dataset_model(name, catalog.dataset_uri(name))
            for name in catalog.list_datasets()
        ]
        smart_write_models(out_uri, datasets)


@cli.command("configure")
def cli_configure(
    config: Annotated[
        str, typer.Option("-c", help="Configuration yml to store as `config.yml`")
    ],
) -> None:
    """Update the dataset configuration from a yaml file.

    Merges into the existing ``config.yml`` and keeps a versioned snapshot.
    Layout-affecting settings (``shards``) only take effect on a dataset that
    has not been written to yet.
    """
    with DatasetContext() as (name, uri):
        console.print(write_config(name, uri, config))


# Import submodules so their sub-typers and commands get registered on `cli`.
from ftm_lakehouse.cli import (  # noqa: E402, F401
    archive,
    crawl,
    entities,
    maintenance,
    statements,
    zfs,
)
