"""Entity read/write commands for the CLI.

Provides ``write-entities`` (bulk ingest from FtM JSON) and
``stream-entities`` (export to FtM JSON).
"""

from typing import Annotated

from ftmq.io import smart_read_proxies, smart_write_proxies
from typer import Option

from ftm_lakehouse.cli import DatasetContext, cli


@cli.command("write-entities")
def cli_write_entities(
    in_uri: Annotated[str, Option("-i")] = "-",
    flush: Annotated[
        bool, Option(help="Flush journal to parquet after writing")
    ] = False,
):
    """Write FtM entities from an input source into the statement store."""
    with DatasetContext() as dataset:
        with dataset.entities.bulk(origin="bulk") as writer:
            for proxy in smart_read_proxies(in_uri):
                writer.add_entity(proxy)
        if flush:
            dataset.entities.flush()


@cli.command("stream-entities")
def cli_stream_entities(
    out_uri: Annotated[str, Option("-o")] = "-",
):
    """Stream FtM entities from ``entities.ftm.json`` to an output sink."""
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.entities.stream())
