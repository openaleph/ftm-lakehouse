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
from ftm_lakehouse.catalog import Catalog
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.dataset import Dataset
from ftm_lakehouse.lake import get_catalog, get_dataset
from ftm_lakehouse.operation.crawl import crawl
from ftm_lakehouse.operation.export import (
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.operation.optimize import OptimizeJob, OptimizeOperation

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
            STATE["catalog"] = get_catalog()
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
        STATE["dataset"].ensure()
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
    catalog = get_catalog(uri)
    STATE["catalog"] = catalog
    if dataset:
        # if dataset_uri:
        #     STATE["dataset"] = get_dataset(dataset, dataset_uri)
        # else:
        STATE["dataset"] = get_dataset(dataset)
    if settings:
        console.print(settings_)
        console.print(STATE)
        raise typer.Exit()


@cli.command("ls")
def cli_dataset_names(out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Show list of dataset names in the current catalog
    """
    with CatalogContext() as catalog:
        names = [d.name for d in catalog.list_datasets()]
        smart_write(out_uri, "\n".join(names) + "\n", "wb")


@cli.command("datasets")
def cli_datasets(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Show metadata for all existing datasets in the current catalog
    """
    with CatalogContext() as catalog:
        datasets = [d.model for d in catalog.list_datasets()]
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
    Make or update a dataset. Use --full for a full update including
    flushing the journal and generating all exports.
    """
    with DatasetContext() as dataset:
        # Flush journal first
        dataset.entities.flush()

        if full:
            # Export statements
            job = ExportStatementsJob.make(dataset=dataset.name)
            op = ExportStatementsOperation(
                job=job,
                entities=dataset.entities,
                jobs=dataset.jobs,
            )
            op.run()

            # Export entities
            job = ExportEntitiesJob.make(dataset=dataset.name)
            op = ExportEntitiesOperation(
                job=job,
                entities=dataset.entities,
                jobs=dataset.jobs,
            )
            op.run()

            # Export statistics
            job = ExportStatisticsJob.make(dataset=dataset.name)
            op = ExportStatisticsOperation(
                job=job,
                entities=dataset.entities,
                jobs=dataset.jobs,
            )
            op.run()

        # Export index
        job = ExportIndexJob.make(
            dataset=dataset.name,
            include_statements_csv=full,
            include_entities_json=full,
            include_statistics=full,
        )
        op = ExportIndexOperation(
            job=job,
            entities=dataset.entities,
            jobs=dataset.jobs,
        )
        op.run(dataset=dataset.model)
        console.print(dataset.model)


@cli.command("write-entities")
def cli_write_entities(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
):
    """
    Write entities to the statement store
    """
    with DatasetContext() as dataset:
        with dataset.entities.bulk(origin="bulk") as writer:
            for proxy in smart_read_proxies(in_uri):
                writer.add_entity(proxy)
        dataset.entities.flush()


@cli.command("stream-entities")
def cli_stream_entities(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Stream entities from `entities.ftm.json`
    """
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.entities.stream())


@cli.command("export-statements")
def cli_export_statements():
    """
    Export statement store to sorted `statements.csv`
    """
    with DatasetContext() as dataset:
        job = ExportStatementsJob.make(dataset=dataset.name)
        op = ExportStatementsOperation(
            job=job,
            entities=dataset.entities,
            jobs=dataset.jobs,
        )
        op.run()
        console.print("Exported statements.csv")


@cli.command("export-entities")
def cli_export_entities():
    """
    Export `statements.csv` to `entities.json`
    """
    with DatasetContext() as dataset:
        # Export statements first
        job = ExportStatementsJob.make(dataset=dataset.name)
        op = ExportStatementsOperation(
            job=job,
            entities=dataset.entities,
            jobs=dataset.jobs,
        )
        op.run()

        # Then export entities
        job = ExportEntitiesJob.make(dataset=dataset.name)
        op = ExportEntitiesOperation(
            job=job,
            entities=dataset.entities,
            jobs=dataset.jobs,
        )
        op.run()
        console.print("Exported entities.ftm.json")


@cli.command("optimize")
def cli_optimize(
    vacuum: Annotated[
        Optional[bool], typer.Option(help="Delete staled files after optimization")
    ] = False,
):
    """
    Optimize a datasets statement store
    """
    with DatasetContext() as dataset:
        job = OptimizeJob.make(dataset=dataset.name, vacuum=vacuum)
        op = OptimizeOperation(
            job=job,
            entities=dataset.entities,
            jobs=dataset.jobs,
        )
        op.run()
        console.print("Optimized statement store")


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
    with DatasetContext() as dataset:
        file = dataset.archive.get(content_hash)
        with dataset.archive.open(file) as i, smart_open(out_uri, "wb") as o:
            o.write(i.read())


@archive.command("head")
def cli_archive_head(
    content_hash: str, out_uri: Annotated[str, typer.Option("-o")] = "-"
):
    """
    Retrieve a file info from dataset archive and write to out uri (default: stdout)
    """
    with DatasetContext() as dataset:
        file = dataset.archive.get(content_hash)
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
    with DatasetContext() as dataset:
        iterator = dataset.archive.iterate()
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
    exclude: Annotated[
        Optional[str], typer.Option(help="Exclude paths glob pattern")
    ] = None,
    include: Annotated[
        Optional[str], typer.Option(help="Include paths glob pattern")
    ] = None,
    make_entities: Annotated[
        Optional[bool], typer.Option(help="Create entities from crawled files")
    ] = True,
):
    """
    Crawl documents from local or remote sources
    """
    with DatasetContext() as dataset:
        result = crawl(
            dataset.name,
            uri,
            archive=dataset.archive,
            entities=dataset.entities,
            jobs=dataset.jobs,
            glob=include,
            exclude_glob=exclude,
            make_entities=make_entities,
        )
        write_obj(result, out_uri)


@mappings.command("ls")
def cli_mappings_ls(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    List all mapping configurations in the dataset
    """
    with DatasetContext() as dataset:
        hashes = list(dataset.mappings.list())
        smart_write(out_uri, "\n".join(hashes) + "\n" if hashes else "", "wb")


@mappings.command("get")
def cli_mappings_get(
    content_hash: str,
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Get a mapping configuration by content hash
    """
    with DatasetContext() as dataset:
        mapping = dataset.mappings.get(content_hash)
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
    with DatasetContext() as dataset:
        if content_hash:
            job = MappingJob.make(dataset=dataset.name, content_hash=content_hash)
            op = MappingOperation(
                job=job,
                archive=dataset.archive,
                entities=dataset.entities,
                jobs=dataset.jobs,
            )
            result = op.run()
            console.print(f"Generated {result.done} entities from {content_hash}")
        else:
            total = 0
            count = 0
            for mapping_hash in dataset.mappings.list():
                job = MappingJob.make(dataset=dataset.name, content_hash=mapping_hash)
                op = MappingOperation(
                    job=job,
                    archive=dataset.archive,
                    entities=dataset.entities,
                    jobs=dataset.jobs,
                )
                result = op.run()
                if result.done > 0:
                    console.print(f"{mapping_hash}: {result.done} entities")
                total += result.done
                count += 1
            console.print(f"Total: {total} entities from {count} mappings")
