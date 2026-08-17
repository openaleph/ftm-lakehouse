"""Entity read/write commands for the CLI.

Sub-typer group:

    ftm-lakehouse entities iterate   # parquet -> FtM JSON (live read)
    ftm-lakehouse entities stream    # entities.ftm.json -> stdout (frozen export)
    ftm-lakehouse entities import    # FtM JSON -> parquet (bypasses journal)
"""

from anystore.io.read import smart_stream_json
from ftmq.io import smart_read_proxies, smart_write_proxies

from ftm_lakehouse.cli import (
    OPT_BULK_SIZE,
    OPT_IN,
    OPT_LAST_SEEN,
    OPT_ORIGIN,
    OPT_OUT,
    OPT_OVERRIDE_ORIGIN,
    OPT_UNSAFE,
    DatasetContext,
    settings,
    sub_typer,
)
from ftm_lakehouse.cli.io import (
    BULK_ORIGIN,
    import_entities,
    import_entities_unsafe,
    stream_export,
)
from ftm_lakehouse.repository.factories import get_entities

entities = sub_typer("entities", "Read and write FtM entities")


@entities.command("iterate")
def cli_entities_iterate(
    out_uri: OPT_OUT = "-",
):
    """Iterate entities from the parquet store as FtM JSON lines.

    Live read – reflects current state of the parquet table post-flush, but
    correctness is only guaranteed after ``maintenance optimize``. For the
    frozen pre-exported view use ``stream``.
    """
    with DatasetContext() as (name, uri):
        smart_write_proxies(out_uri, get_entities(name, uri).query())


@entities.command("stream")
def cli_entities_stream(
    out_uri: OPT_OUT = "-",
):
    """Stream FtM entities from the pre-exported ``entities.ftm.json``."""
    with DatasetContext() as (name, uri):
        repo = get_entities(name, uri)
        stream_export(repo, repo.ENTITIES_JSON, out_uri)


@entities.command("import")
def cli_entities_import(
    in_uri: OPT_IN = "-",
    origin: OPT_ORIGIN = BULK_ORIGIN,
    override_origin: OPT_OVERRIDE_ORIGIN = False,
    bulk_size: OPT_BULK_SIZE = settings.max_buffer_rows,
    last_seen: OPT_LAST_SEEN = None,
    unsafe: OPT_UNSAFE = False,
):
    """
    Bulk-import FtM entities straight into the parquet store, bypassing the
    Journal.

    Can as well take unsorted fragments as input for migration from
    ``followthemoney-store`` into ``ftm-lakehouse`` keeping fragments and origin
    provenance. (Use ``ftmq fragments iterate-fragments -d ...`` for export.)
    """
    with DatasetContext() as (name, uri):
        repo = get_entities(name, uri)
        if unsafe:
            import_entities_unsafe(
                repo,
                smart_stream_json(in_uri),
                origin=origin,
                override_origin=override_origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
        else:
            import_entities(
                repo,
                smart_read_proxies(in_uri),
                origin=origin,
                override_origin=override_origin,
                bulk_size=bulk_size,
                last_seen=last_seen,
            )
