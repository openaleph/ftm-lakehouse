"""Entity read/write commands for the CLI.

Provides ``write-entities`` (bulk ingest from FtM JSON, bypassing the
journal) and ``stream-entities`` (export to FtM JSON).
"""

from datetime import datetime, timezone
from typing import Annotated, Optional

from anystore.io import logged_items
from ftmq.io import smart_read_proxies, smart_write_proxies
from typer import Option

from ftm_lakehouse.cli import DatasetContext, cli
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

BULK_ORIGIN = "bulk"
BULK_SIZE = 1_000_000


@cli.command("write-entities")
def cli_write_entities(
    in_uri: Annotated[str, Option("-i")] = "-",
    origin: Annotated[str, Option(help="Data origin")] = BULK_ORIGIN,
    bulk_size: Annotated[
        int,
        Option(help="Number of statements buffered before flush to parquet."),
    ] = BULK_SIZE,
    last_seen: Annotated[
        Optional[datetime],
        Option(help="Default last_seen timestamp if entity payload has none"),
    ] = None,
):
    """Bulk-import FtM entities straight into the parquet store.

    Bypasses the journal — statements go through an in-memory ``EntityBuffer``
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


@cli.command("stream-entities")
def cli_stream_entities(
    out_uri: Annotated[str, Option("-o")] = "-",
):
    """Stream FtM entities from ``entities.ftm.json`` to an output sink."""
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.entities.stream())
