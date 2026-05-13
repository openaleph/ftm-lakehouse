"""Raw statement read/write commands for the CLI.

Sub-typer group, parallel to ``entities`` but at the statement grain:

    ftm-lakehouse statements iterate   # parquet -> statements CSV (live read)
    ftm-lakehouse statements stream    # statements.csv export -> stdout
    ftm-lakehouse statements import    # statements CSV -> parquet (no journal)
"""

from datetime import datetime, timezone
from typing import Annotated, Optional

import typer
from anystore.io import (
    logged_items,
    smart_open,
    smart_stream_csv,
    smart_write_csv,
    stream_bytes,
)
from followthemoney import Statement

from ftm_lakehouse.cli import DatasetContext, cli, settings
from ftm_lakehouse.cli.entities import BULK_ORIGIN, BULK_SIZE
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

statements = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(statements, name="statements", help="Read and write raw FtM statements")


@statements.command("iterate")
def cli_statements_iterate(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Iterate statements from the parquet store as CSV rows.

    Live read — reflects current state of the parquet table. For the frozen
    pre-exported view use ``stream``.
    """
    with DatasetContext() as dataset:
        rows = dataset.entities._statements._query_statement_data()
        with smart_open(out_uri, "w") as fh:
            smart_write_csv(fh, rows)


@statements.command("stream")
def cli_statements_stream(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Stream the pre-exported ``statements.csv`` to the output."""
    with DatasetContext() as dataset:
        in_uri = dataset._store.to_uri(path.EXPORTS_STATEMENTS)
        with smart_open(in_uri, "rb") as i, smart_open(out_uri, "wb") as o:
            for chunk in stream_bytes(i):
                o.write(chunk)


@statements.command("import")
def cli_statements_import(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    origin: Annotated[str, typer.Option(help="Data origin")] = BULK_ORIGIN,
    bulk_size: Annotated[
        int,
        typer.Option(help="Number of statements buffered before flush to parquet."),
    ] = BULK_SIZE,
    last_seen: Annotated[
        Optional[datetime],
        typer.Option(help="Default last_seen timestamp if row has none"),
    ] = None,
):
    """Bulk-import raw statements (CSV) straight into the parquet store.

    Mirrors ``entities import`` at the statement grain: each input row is
    parsed as a ``Statement``, buffered in ``EntityBuffer`` to pre-sort by
    shard, then handed to ``EntityRepository.write_statements`` for a
    per-shard parquet append. Bypasses the journal.
    """
    with DatasetContext() as dataset:
        repo = dataset.entities
        buffer = EntityBuffer(dataset.name, dataset.model.shards, origin)
        now = last_seen or datetime.now(timezone.utc)

        for row in logged_items(
            smart_stream_csv(in_uri),
            "Write",
            item_name="Statement",
            logger=dataset._log,
        ):
            stmt = Statement.from_dict(row)
            buffer.add_statement(stmt)
            if len(buffer) >= bulk_size:
                repo.write_statements(buffer.flush_buffer(), now=now)

        if buffer:
            repo.write_statements(buffer.flush_buffer(), now=now)
