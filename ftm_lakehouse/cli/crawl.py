"""Crawl documents into a dataset via the CLI.

ftm-lakehouse -d <dataset> crawl ...
"""

from typing import Annotated, Optional

import typer

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import DatasetContext, cli, write_obj
from ftm_lakehouse.operation.crawl import HandleExistingMode


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
    with DatasetContext() as (name, dataset_uri):
        result = op.crawl(
            name,
            uri,
            glob=include,
            exclude_glob=exclude,
            make_entities=make_entities,
            existing=existing,
            uri=dataset_uri,
        )
        write_obj(result, out_uri)
