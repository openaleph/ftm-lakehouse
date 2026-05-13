"""Entity read/write commands for the CLI.

Sub-typer group:

    ftm-lakehouse entities iterate   # parquet -> FtM JSON (live read)
    ftm-lakehouse entities stream    # entities.ftm.json -> stdout (frozen export)
    ftm-lakehouse entities import    # FtM JSON -> parquet (bypasses journal)
"""

from datetime import datetime, timezone
from typing import Annotated, Optional

import typer
from anystore.io import logged_items
from ftmq.io import smart_read_proxies, smart_write_proxies

from ftm_lakehouse.cli import DatasetContext, cli, settings
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

BULK_ORIGIN = "bulk"
BULK_SIZE = 1_000_000


entities = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(entities, name="entities", help="Read and write FtM entities")


@entities.command("iterate")
def cli_entities_iterate(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Iterate entities from the parquet store as FtM JSON lines.

    Live read – reflects current state of the parquet table (post-flush,
    post-merge). For the frozen pre-exported view use ``stream``.
    """
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.entities.query())


@entities.command("stream")
def cli_entities_stream(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Stream FtM entities from the pre-exported ``entities.ftm.json``."""
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.entities.stream())


@entities.command("import")
def cli_entities_import(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    origin: Annotated[str, typer.Option(help="Data origin")] = BULK_ORIGIN,
    bulk_size: Annotated[
        int,
        typer.Option(help="Number of statements buffered before flush to parquet."),
    ] = BULK_SIZE,
    last_seen: Annotated[
        Optional[datetime],
        typer.Option(help="Default last_seen timestamp if entity payload has none"),
    ] = None,
):
    """Bulk-import FtM entities straight into the parquet store.

    Bypasses the journal – statements go through an in-memory ``EntityBuffer``
    that pre-sorts by shard, then ``EntityRepository.write_statements`` packs
    them per-shard into the parquet store. Intended for one-shot loads of
    large ``entities.ftm.json`` files where journal write-amplification would
    be wasteful.
    """
    with DatasetContext() as dataset:
        repo = dataset.entities
        buffer = EntityBuffer(dataset.name, dataset.model.shards, origin)
        now = last_seen or datetime.now(timezone.utc)

        for proxy in logged_items(
            smart_read_proxies(in_uri),
            "Write",
            item_name="Entity",
            logger=dataset._log,
        ):
            buffer.add_entity(proxy)
            if len(buffer) >= bulk_size:
                repo.write_statements(buffer.flush_buffer(), now=now)

        if buffer:
            repo.write_statements(buffer.flush_buffer(), now=now)
