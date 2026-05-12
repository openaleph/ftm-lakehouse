"""Dataset operation commands for the CLI.

Provides commands for building, exporting, optimizing, and crawling
datasets: ``make``, ``export-statements``, ``export-entities``,
``export-statistics``, ``export-documents``, ``compact``, ``merge``,
``vacuum``, and ``crawl``.
"""

from typing import Annotated, Optional

import typer

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import DatasetContext, cli, console, write_obj
from ftm_lakehouse.model.dataset import DatasetModel
from ftm_lakehouse.operation.crawl import HandleExistingMode


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
            dataset.entities.flush()
            op.export_index(dataset, force=bool(force))
        console.print(dataset.index)


@cli.command("export-statements")
def cli_export_statements():
    """Export the statement store to a sorted ``statements.csv``."""
    with DatasetContext() as dataset:
        op.export_statements(dataset)
        console.print("Exported statements.csv")


@cli.command("export-entities")
def cli_export_entities():
    """Export the statement store to ``entities.ftm.json``."""
    with DatasetContext() as dataset:
        op.export_entities(dataset)
        console.print("Exported entities.ftm.json")


@cli.command("export-statistics")
def cli_export_statistics():
    """Export statement store statistics to ``statistics.json``."""
    with DatasetContext() as dataset:
        op.export_statistics(dataset)
        console.print("Exported statistics.json")


@cli.command("export-documents")
def cli_export_documents():
    """Export document metadata to ``documents.csv``."""
    with DatasetContext() as dataset:
        op.export_documents(dataset)
        console.print("Exported documents.csv")


@cli.command("compact")
def cli_compact(
    force: Annotated[
        Optional[bool], typer.Option(help="Run regardless of freshness state.")
    ] = False,
):
    """Bin-pack small parquet files (cheap maintenance).

    Holds ``locks/lakehouse/compact`` for the duration. Does not collapse
    duplicate rows or drop tombstones — use ``merge`` for that.
    """
    with DatasetContext() as dataset:
        res = op.compact(dataset, force=bool(force))
        console.print(res)


@cli.command("merge")
def cli_merge(
    force: Annotated[
        Optional[bool], typer.Option(help="Run regardless of freshness state.")
    ] = False,
):
    """Collapse duplicates and reap expired tombstones per partition.

    Expensive — overwrites each ``(shard, bucket, origin)`` partition with a
    deduplicated view. Tombstones older than ``LAKEHOUSE_GRACE_PERIOD_DAYS``
    are dropped. Holds ``locks/lakehouse/merge`` for the duration.
    """
    with DatasetContext() as dataset:
        res = op.merge(dataset, force=bool(force))
        console.print(res)


@cli.command("vacuum")
def cli_vacuum(
    retention_hours: Annotated[
        Optional[int],
        typer.Option(help="Retain files newer than this many hours."),
    ] = 0,
    force: Annotated[
        Optional[bool], typer.Option(help="Run regardless of freshness state.")
    ] = False,
):
    """Delete obsolete parquet files no longer referenced by the Delta log.

    Holds ``locks/lakehouse/vacuum`` for the duration.
    """
    with DatasetContext() as dataset:
        res = op.vacuum(
            dataset, retention_hours=int(retention_hours or 0), force=bool(force)
        )
        console.print(res)


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
