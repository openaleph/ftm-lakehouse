"""Raw statement read/write commands for the CLI.

Sub-typer group, parallel to ``entities`` but at the statement grain:

    ftm-lakehouse statements iterate   # parquet -> statements CSV (live read)
    ftm-lakehouse statements stream    # statements.csv export -> stdout
    ftm-lakehouse statements import    # statements CSV -> parquet (no journal)
"""

from datetime import datetime
from typing import Annotated, Optional

import typer
from anystore.io import smart_open, smart_write_csv
from anystore.io.read import smart_stream_csv
from anystore.logic.io import stream
from anystore.util import Took
from rich.console import Console
from rich.table import Table

from ftm_lakehouse.cli import DatasetContext, cli, settings
from ftm_lakehouse.cli.io import (
    BULK_ORIGIN,
    import_statements,
    import_statements_unsafe,
)
from ftm_lakehouse.helpers.statements import read_csv_statements
from ftm_lakehouse.logic.compress import decompress_stream
from ftm_lakehouse.repository.factories import get_entities

statements = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(statements, name="statements", help="Read and write raw FtM statements")


@statements.command("iterate")
def cli_statements_iterate(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Iterate statements from the parquet store as CSV rows.

    Live read – reflects current state of the parquet table. For the frozen
    pre-exported view use ``stream``.
    """
    with DatasetContext() as (name, uri):
        entities = get_entities(name, uri)
        if entities._is_api:
            rows = (r.to_dict() for r in entities.query_statements())
        else:
            # faster as no Statement model serialization
            rows = entities._statements._query_statement_data()
        with smart_open(out_uri, "w") as fh:
            smart_write_csv(fh, rows)


@statements.command("stream")
def cli_statements_stream(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Stream the pre-exported ``statements.csv`` to the output."""
    with DatasetContext() as (name, uri):
        # we trust our exports so stream byte-to-byte directly instead the
        # python / ftm roundtrip
        entities = get_entities(name, uri)
        in_uri = entities._store.to_uri(entities.EXPORTS_STATEMENTS)
        with (
            smart_open(in_uri, "rb") as fh,
            decompress_stream(fh, entities.compression) as i,
            smart_open(out_uri, "wb") as o,
        ):
            stream(i, o)


@statements.command("import")
def cli_statements_import(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    origin: Annotated[str, typer.Option(help="Data origin")] = BULK_ORIGIN,
    bulk_size: Annotated[
        int,
        typer.Option(help="Number of statements buffered before flush to parquet."),
    ] = settings.max_buffer_rows,
    last_seen: Annotated[
        Optional[datetime],
        typer.Option(help="Default last_seen timestamp if row has none"),
    ] = None,
    unsafe: Annotated[
        bool,
        typer.Option(
            "--unsafe",
            help="Fast path: map CSV rows straight to parquet rows, skipping "
            "Statement object construction and validation. Trusted input "
            "only.",
        ),
    ] = False,
):
    """Bulk-import raw statements (CSV) straight into the parquet store.

    Mirrors ``entities import`` at the statement grain. Rows are parsed with
    the lakehouse ``read_csv_statements`` – which preserves the ``fragment``
    supersession key (followthemoney's reader has no notion of it) – then
    buffered in ``EntityBuffer`` to pre-sort by shard and handed to
    ``EntityRepository.write_statements`` for a per-shard parquet append.
    Bypasses the journal. With ``--unsafe``, rows skip Statement
    construction entirely and map straight to parquet rows.
    """
    with DatasetContext() as (name, uri):
        repo = get_entities(name, uri)
        if unsafe:
            import_statements_unsafe(
                repo,
                smart_stream_csv(in_uri),
                origin=origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
        else:
            import_statements(
                repo,
                read_csv_statements(in_uri),
                origin=origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )


@statements.command("sql")
def cli_statements_sql(query: str):
    """Run a raw SQL query against the parquet store, rendered as a table.

    Queries the registered DuckDB views – ``statement`` (deduped-live) and
    ``statement_raw`` (physical rows). Results print as a rich table; add a
    ``LIMIT`` when scanning large partitions.
    """
    with DatasetContext() as (name, uri):
        store = get_entities(name, uri)._statements._lake
        with store.cursor() as cur:
            with Took() as t:
                cur.execute(query)
            Console().print(f"Query took: {t.took}")
            if cur.description is None:  # statement with no result set
                return
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
    table = Table(*columns, caption=f"{len(rows)} row(s)")
    for row in rows:
        table.add_row(*("" if value is None else str(value) for value in row))
    Console().print(table)
