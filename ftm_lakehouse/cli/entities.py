"""Entity read/write commands for the CLI.

Provides ``write-entities`` (bulk ingest from FtM JSON) and
``stream-entities`` (export to FtM JSON).
"""

from typing import Annotated

from anystore.io import logged_items
from ftmq.io import smart_read_proxies, smart_write_proxies
from typer import Option

from ftm_lakehouse.cli import DatasetContext, cli

BULK_ORIGIN = "bulk"


@cli.command("write-entities")
def cli_write_entities(
    in_uri: Annotated[str, Option("-i")] = "-",
    flush: Annotated[
        bool, Option(help="Flush journal to parquet after writing")
    ] = False,
    origin: Annotated[str, Option(..., help="Data origin")] = BULK_ORIGIN,
):
    """Write FtM entities from an input source into the journal."""
    with DatasetContext() as dataset:
        with dataset.entities.writer(origin=origin) as writer:
            for proxy in logged_items(
                smart_read_proxies(in_uri),
                "Write",
                item_name="Entity",
                logger=dataset._log,
                journal=dataset.entities._journal.uri,
            ):
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
