"""Entity read/write commands for the CLI.

Sub-typer group:

    ftm-lakehouse entities iterate   # parquet -> FtM JSON (live read)
    ftm-lakehouse entities stream    # entities.ftm.json -> stdout (frozen export)
    ftm-lakehouse entities import    # FtM JSON -> parquet (bypasses journal)
"""

from datetime import datetime
from typing import Annotated, Optional

import typer
from anystore.io import smart_open
from anystore.io.read import smart_stream_json
from anystore.logic.io import stream
from ftmq.io import smart_read_proxies, smart_write_proxies

from ftm_lakehouse.cli import DatasetContext, cli, settings
from ftm_lakehouse.cli.io import BULK_ORIGIN, import_entities, import_entities_unsafe
from ftm_lakehouse.logic.compress import decompress_stream

entities = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(entities, name="entities", help="Read and write FtM entities")


@entities.command("iterate")
def cli_entities_iterate(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Iterate entities from the parquet store as FtM JSON lines.

    Live read – reflects current state of the parquet table post-flush, but
    correctness is only guaranteed after ``maintenance optimize``. For the
    frozen pre-exported view use ``stream``.
    """
    with DatasetContext() as dataset:
        smart_write_proxies(out_uri, dataset.get_entities().query())


@entities.command("stream")
def cli_entities_stream(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Stream FtM entities from the pre-exported ``entities.ftm.json``."""
    with DatasetContext() as dataset:
        # we trust our exports so stream byte-to-byte directly instead the
        # python / ftm roundtrip
        entities = dataset.get_entities()
        in_uri = dataset._store.to_uri(entities.ENTITIES_JSON)
        with (
            smart_open(in_uri, "rb") as fh,
            decompress_stream(fh, entities.compression) as i,
            smart_open(out_uri, "wb") as o,
        ):
            stream(i, o)


@entities.command("import")
def cli_entities_import(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    origin: Annotated[
        str,
        typer.Option(
            help="Default data origin if missing or multiple in entity payload"
        ),
    ] = BULK_ORIGIN,
    override_origin: Annotated[
        bool, typer.Option(help="Override entity payload origin")
    ] = False,
    bulk_size: Annotated[
        int,
        typer.Option(help="Number of statements buffered before flush to parquet."),
    ] = settings.max_buffer_rows,
    last_seen: Annotated[
        Optional[datetime],
        typer.Option(help="Default last_seen timestamp if entity payload has none"),
    ] = None,
    unsafe: Annotated[
        bool,
        typer.Option(
            "--unsafe",
            help="Fast path: explode entity JSON straight into parquet rows, "
            "skipping FtM object construction and validation. Trusted input "
            "only.",
        ),
    ] = False,
):
    """
    Bulk-import FtM entities straight into the parquet store, bypassing the
    Journal.

    Can as well take unsorted fragments as input for migration from
    ``followthemoney-store`` into ``ftm-lakehouse`` keeping fragments and origin
    provenance. (Use ``ftmq fragments iterate-fragments -d ...`` for export.)
    """
    with DatasetContext() as dataset:
        if unsafe:
            import_entities_unsafe(
                dataset,
                smart_stream_json(in_uri),
                origin=origin,
                override_origin=override_origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
        else:
            import_entities(
                dataset,
                smart_read_proxies(in_uri),
                origin=origin,
                override_origin=override_origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
