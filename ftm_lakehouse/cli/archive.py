"""File archive commands for the CLI.

Provides the ``archive`` sub-command group with operations to retrieve,
inspect, list, and download files stored in the content-addressed archive.
"""

from typing import Annotated

import typer
from anystore.io import smart_open, smart_write_models
from anystore.util import dump_json_model

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import DatasetContext, cli, console, settings

archive = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(archive, name="archive", help="Access the file archive")


@archive.command("get")
def cli_archive_get(
    content_hash: str, out_uri: Annotated[str, typer.Option("-o")] = "-"
):
    """Retrieve a file by content hash and write it to an output URI."""
    with DatasetContext() as dataset:
        file = dataset.archive.get_file(content_hash)
        with dataset.archive.open(file.checksum) as i, smart_open(out_uri, "wb") as o:
            o.write(i.read())


@archive.command("head")
def cli_archive_head(
    content_hash: str, out_uri: Annotated[str, typer.Option("-o")] = "-"
):
    """Retrieve all metadata objects for a content hash and write them out."""
    with DatasetContext() as dataset:
        smart_write_models(out_uri, dataset.archive.get_all_files(content_hash))


@archive.command("ls")
def cli_archive_ls(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
    keys: Annotated[bool, typer.Option(help="Show only keys")] = False,
    checksums: Annotated[bool, typer.Option(help="Show only checksums")] = False,
):
    """List all files in the dataset archive."""
    with DatasetContext() as dataset:
        iterator = dataset.archive.iterate_files()
        if keys:
            files = (f.key.encode() + b"\n" for f in iterator)
        elif checksums:
            files = (f.checksum.encode() + b"\n" for f in iterator)
        else:
            files = (dump_json_model(f, newline=True) for f in iterator)
        with smart_open(out_uri, "wb") as o:
            o.writelines(files)


@archive.command("download")
def cli_archive_download(target: Annotated[str, typer.Option("-o")]):
    """Download all archive files to a local directory."""
    with DatasetContext() as dataset:
        res = op.download_archive(dataset, target)
        console.print(res)
