"""Raw statement read/write commands for the CLI.

Sub-typer group, parallel to ``entities`` but at the statement grain:

    ftm-lakehouse statements iterate   # parquet -> statements CSV (live read)
    ftm-lakehouse statements stream    # statements.csv export -> stdout
    ftm-lakehouse statements import    # statements CSV -> parquet (no journal)
"""

from anystore import smart_open
from anystore.io import smart_write_csv
from anystore.io.read import smart_stream_csv
from anystore.util import Took
from rich.console import Console
from rich.table import Table

from ftm_lakehouse.cli import (
    OPT_BULK_SIZE,
    OPT_IN,
    OPT_LAST_SEEN,
    OPT_ORIGIN,
    OPT_OUT,
    OPT_OVERRIDE_ORIGIN,
    OPT_ROLE,
    OPT_UNSAFE,
    DatasetContext,
    settings,
    sub_typer,
)
from ftm_lakehouse.cli.io import (
    BULK_ORIGIN,
    import_statements,
    import_statements_unsafe,
    stream_export,
)
from ftm_lakehouse.model.statement import read_csv_statements
from ftm_lakehouse.repository.factories import get_entities

statements = sub_typer("statements", "Read and write raw FtM statements")


@statements.command("iterate")
def cli_statements_iterate(out_uri: OPT_OUT = "-"):
    """Iterate statements from the parquet store as CSV rows.

    Live read – reflects current state of the parquet table. For the frozen
    pre-exported view use ``stream``.
    """
    with DatasetContext() as (name, uri):
        entities = get_entities(name, uri)
        # raw row dicts – no Statement model serialization on either backend
        rows = entities.query_statements_data()
        with smart_open(out_uri, "w") as fh:
            smart_write_csv(fh, rows)


@statements.command("stream")
def cli_statements_stream(out_uri: OPT_OUT = "-"):
    """Stream the pre-exported ``statements.csv`` to the output."""
    with DatasetContext() as (name, uri):
        repo = get_entities(name, uri)
        stream_export(repo, repo.EXPORTS_STATEMENTS, out_uri)


@statements.command("import")
def cli_statements_import(
    in_uri: OPT_IN = "-",
    origin: OPT_ORIGIN = BULK_ORIGIN,
    override_origin: OPT_OVERRIDE_ORIGIN = False,
    role: OPT_ROLE = None,
    bulk_size: OPT_BULK_SIZE = settings.max_buffer_rows,
    last_seen: OPT_LAST_SEEN = None,
    unsafe: OPT_UNSAFE = False,
):
    """Bulk-import raw statements (CSV) straight into the parquet store.

    Mirrors ``entities import`` at the statement grain. Rows are parsed with
    the lakehouse ``read_csv_statements`` – which preserves the ``fragment``
    supersession key (followthemoney's reader has no notion of it) – then
    buffered in ``EntityBuffer`` and handed to
    ``EntityRepository.write_batches`` as one packed table. Bypasses the
    journal. With ``--unsafe``, rows skip Statement
    construction entirely and map straight to parquet rows.
    """
    with DatasetContext() as (name, uri):
        repo = get_entities(name, uri)
        if unsafe:
            import_statements_unsafe(
                repo,
                smart_stream_csv(in_uri),
                origin=origin,
                override_origin=override_origin,
                role=role,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
        else:
            import_statements(
                repo,
                read_csv_statements(in_uri),
                origin=origin,
                override_origin=override_origin,
                role=role,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )


@statements.command("sql")
def cli_statements_sql(query: str):
    """Run a raw SQL query against the parquet store, rendered as a table.

    Queries the registered DuckDB views – ``statement`` (deduped-live) and
    ``statement_raw`` (physical rows). Results print as a rich table; add a
    ``LIMIT`` when scanning large partitions.

    Local-only: raw SQL is deliberately not exposed over the API – run this
    where the storage is directly accessible.
    """
    with DatasetContext() as (name, uri):
        entities = get_entities(name, uri)
        if entities._is_api:
            raise RuntimeError("`statements sql` is not available in API mode")
        store = entities._statements._lake
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
