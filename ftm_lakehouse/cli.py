from typing import Annotated, Optional, TypedDict

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_open, smart_write, smart_write_models
from anystore.logging import configure_logging
from anystore.util import dump_json_model
from ftmq.io import smart_read_proxies, smart_write_proxies
from pydantic import BaseModel
from rich.console import Console

from ftm_lakehouse import __version__
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.io import ensure_dataset, write_entities
from ftm_lakehouse.lake import DatasetLakehouse, Lakehouse, get_lakehouse
from ftm_lakehouse.logic.crawl import crawl

settings = Settings()
cli = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=settings.debug,
    name="FollowTheMoney Data Lakehouse",
)
archive = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(archive, name="archive", help="Access the file archive")
mappings = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(mappings, name="mappings", help="Manage and process data mappings")
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
        lake = STATE["lakehouse"]
        assert lake is not None
        return lake


class Dataset(ErrorHandler):
    def __enter__(self) -> DatasetLakehouse:
        super().__enter__()
        if not STATE["dataset"]:
            e = RuntimeError("Specify dataset name with `-d` option!")
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
    lake = get_lakehouse(uri)
    STATE["lakehouse"] = lake
    if dataset:
        # if dataset_uri:
        #     STATE["dataset"] = get_dataset(dataset, dataset_uri)
        # else:
        STATE["dataset"] = lake.get_dataset(dataset)
    if settings:
        console.print(settings_)
        console.print(STATE)
        raise typer.Exit()


@cli.command("ls")
def cli_dataset_names(out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Show list of dataset names in the current lake
    """
    with Catalog() as lake:
        names = [d.name for d in lake.get_datasets()]
        smart_write(out_uri, "\n".join(names) + "\n", "wb")


@cli.command("datasets")
def cli_datasets(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Show metadata for all existing datasets in the current lake
    """
    with Catalog() as lake:
        datasets = [d.load_model() for d in lake.get_datasets()]
        smart_write_models(out_uri, datasets)


@cli.command("make")
def cli_make(
    full: Annotated[
        Optional[bool],
        typer.Option(
            help="Run full update: flush journal, export statements/entities, compute stats"
        ),
    ] = False,
):
    """
    Make or update a dataset. Use --exports for a full update including
    flushing the journal and generating all exports.
    """
    with Dataset() as dataset:
        if full:
            dataset.make()
        console.print(dataset.make_index(full))


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
        dataset.entities.export_statements()


@cli.command("export-entities")
def cli_export_entities():
    """
    Export `statements.csv` to `entities.json`
    """
    with Dataset() as dataset:
        dataset.entities.export_statements()
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
        dataset.entities.optimize(vacuum)


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


@mappings.command("ls")
def cli_mappings_ls(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    List all mapping configurations in the dataset
    """
    with Dataset() as dataset:
        hashes = list(dataset.mappings.list_mappings())
        smart_write(out_uri, "\n".join(hashes) + "\n" if hashes else "", "wb")


@mappings.command("get")
def cli_mappings_get(
    content_hash: str,
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Get a mapping configuration by content hash
    """
    with Dataset() as dataset:
        mapping = dataset.mappings.get_mapping(content_hash)
        if mapping is None:
            console.print(f"[red]No mapping found for {content_hash}[/red]")
            raise typer.Exit(code=1)
        smart_write(out_uri, dump_json_model(mapping, newline=True))


@mappings.command("process")
def cli_mappings_process(
    content_hash: Annotated[
        Optional[str], typer.Argument(help="Content hash to process (omit for all)")
    ] = None,
):
    """
    Process mapping configuration(s) and generate entities.
    If no content_hash is provided, processes all mappings.
    """
    with Dataset() as dataset:
        if content_hash:
            count = dataset.mappings.process(content_hash)
            console.print(f"Generated {count} entities from {content_hash}")
        else:
            results = dataset.mappings.process_all()
            total = 0
            for h, count in results.items():
                if count > 0:
                    console.print(f"{h}: {count} entities")
                total += count
            console.print(f"Total: {total} entities from {len(results)} mappings")
