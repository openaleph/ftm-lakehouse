from typing import Annotated, Optional, TypedDict

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_open, smart_write
from anystore.logging import configure_logging
from anystore.util import dump_json_model
from ftmq.io import smart_read_proxies, smart_write_proxies
from pydantic import BaseModel
from rich.console import Console

from ftm_lakehouse import __version__
from ftm_lakehouse.crawl import crawl
from ftm_lakehouse.exceptions import ImproperlyConfigured
from ftm_lakehouse.io import ensure_dataset, write_entities
from ftm_lakehouse.lake.base import DatasetLakehouse, Lakehouse, get_lakehouse
from ftm_lakehouse.settings import Settings

settings = Settings()
cli = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=settings.debug,
    name="FollowTheMoney Data Lakehouse",
)
archive = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(archive, name="archive", help="Access the file archive")
console = Console(stderr=True)


class State(TypedDict):
    lakehouse: Lakehouse | None
    dataset: DatasetLakehouse | None


STATE: State = {"lakehouse": None, "dataset": None}


def write_obj(obj: BaseModel | None, out: str) -> None:
    if out == "-":
        console.print(obj)
    else:
        if obj is not None:
            smart_write(out, dump_json_model(obj, clean=True, newline=True))


class Catalog(ErrorHandler):
    def __enter__(self) -> Lakehouse:
        if not STATE["lakehouse"]:
            STATE["lakehouse"] = get_lakehouse()
        return STATE["lakehouse"]


class Dataset(ErrorHandler):
    def __enter__(self) -> DatasetLakehouse:
        super().__enter__()
        if not STATE["dataset"]:
            e = ImproperlyConfigured("Specify dataset name with `-d` option!")
            if settings.debug:
                raise e
            console.print(f"[red][bold]{e.__class__.__name__}[/bold]: {e}[/red]")
            raise typer.Exit(code=1)
        ensure_dataset(STATE["dataset"])
        return STATE["dataset"]


@cli.callback(invoke_without_command=True)
def cli_ftm_lakehouse(
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
    STATE["lakehouse"] = get_lakehouse(uri)
    if dataset:
        # if dataset_uri:
        #     STATE["dataset"] = get_dataset(dataset, dataset_uri)
        # else:
        STATE["dataset"] = STATE["lakehouse"].get_dataset(dataset)
    if settings:
        console.print(settings_)
        console.print(STATE)
        raise typer.Exit()


@cli.command("catalog")
def cli_catalog(
    names: Annotated[
        bool, typer.Option(help="Only show dataset names (`foreign_id`)")
    ] = False,
):
    """
    Show catalog for all existing datasets
    """
    with Catalog() as lake:
        datasets = list(lake.get_datasets())
        if names:
            datasets = [d.name for d in datasets]
        else:
            datasets = [d.load_model() for d in datasets]
        console.print(datasets)


@cli.command("make")
def cli_make(
    compute_stats: Annotated[
        Optional[bool], typer.Option(help="(Re-)compute `statistics.json`")
    ] = False,
    exports: Annotated[
        Optional[bool],
        typer.Option(help="(Re-)generate all exports if their dependencies changed"),
    ] = False,
):
    """
    Make or update a datasets metadata (`config.yml` and `index.json`). Can be
    used to initialize a new dataset.
    """
    with Dataset() as dataset:
        if exports:
            compute_stats = True
            dataset.statements.export()
            dataset.entities.export()
        console.print(dataset.make_index(compute_stats))


@cli.command("write-entities")
def cli_write_entities(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
):
    """
    Write entities to the statement store
    """
    with Dataset() as dataset:
        write_entities(dataset.name, smart_read_proxies(in_uri), origin="bulk")


@cli.command("stream-entities")
def cli_stream_entities(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Stream entities from `entities.ftm.json`
    """
    with Dataset() as dataset:
        smart_write_proxies(out_uri, dataset.entities.iterate())


@cli.command("export-statements")
def cli_export_statements():
    """
    Export statement store to sorted `statements.csv`
    """
    with Dataset() as dataset:
        dataset.statements.export()


@cli.command("export-entities")
def cli_export_entities():
    """
    Export `statements.csv` to `entities.json`
    """
    with Dataset() as dataset:
        dataset.statements.export()
        dataset.entities.export()


@cli.command("optimize")
def cli_optimize(
    vacuum: Annotated[
        Optional[bool], typer.Option(help="Delete staled files after optimization")
    ] = False,
):
    """
    Optimize a datasets statement store
    """
    with Dataset() as dataset:
        dataset.statements.optimize(vacuum)


# @cli.command("versions")
# def cli_versions():
#     """Show versions of dataset"""
#     with Dataset() as dataset:
#         for version in dataset.documents.get_versions():
#             console.print(version)


# @cli.command("diff")
# def cli_diff(
#     version: Annotated[str, typer.Option("-v", help="Version")],
#     out_uri: Annotated[str, typer.Option("-o")] = "-",
# ):
#     """
#     Show documents diff for given version
#     """
#     with Dataset() as dataset:
#         ver = dataset.documents.get_version(version)
#         with smart_open(out_uri, DEFAULT_WRITE_MODE) as out:
#             out.write(ver)


@archive.command("get")
def cli_archive_get(
    content_hash: str, out_uri: Annotated[str, typer.Option("-o")] = "-"
):
    """
    Retrieve a file from dataset archive and write to out uri (default: stdout)
    """
    with Dataset() as dataset:
        file = dataset.archive.lookup_file(content_hash)
        with dataset.archive.open_file(file) as i, smart_open(out_uri, "wb") as o:
            o.write(i.read())


@archive.command("head")
def cli_archive_head(
    content_hash: str, out_uri: Annotated[str, typer.Option("-o")] = "-"
):
    """
    Retrieve a file info from dataset archive and write to out uri (default: stdout)
    """
    with Dataset() as dataset:
        file = dataset.archive.lookup_file(content_hash)
        smart_write(out_uri, dump_json_model(file, newline=True))


@archive.command("ls")
def cli_archive_ls(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
    keys: Annotated[bool, typer.Option(help="Show only keys")] = False,
    checksums: Annotated[bool, typer.Option(help="Show only checksums")] = False,
):
    """
    List all files in dataset archive
    """
    with Dataset() as dataset:
        iterator = dataset.archive.iter_files()
        if keys:
            files = (f.key.encode() + b"\n" for f in iterator)
        elif checksums:
            files = (f.checksum.encode() + b"\n" for f in iterator)
        else:
            files = (dump_json_model(f, newline=True) for f in iterator)
        with smart_open(out_uri, "wb") as o:
            o.writelines(files)


@cli.command("crawl")
def cli_crawl(
    uri: str,
    out_uri: Annotated[
        str, typer.Option("-o", help="Write results to this destination")
    ] = "-",
    skip_existing: Annotated[
        Optional[bool],
        typer.Option(
            help="Skip already existing files (doesn't check actual similarity)"
        ),
    ] = True,
    exclude: Annotated[
        Optional[str], typer.Option(help="Exclude paths glob pattern")
    ] = None,
    include: Annotated[
        Optional[str], typer.Option(help="Include paths glob pattern")
    ] = None,
):
    """
    Crawl documents from local or remote sources
    """
    with Dataset() as dataset:
        write_obj(
            crawl(
                uri,
                dataset,
                skip_existing=skip_existing,
                glob=include,
                exclude_glob=exclude,
            ),
            out_uri,
        )
