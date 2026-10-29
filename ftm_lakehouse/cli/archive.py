"""File archive commands for the CLI.

Provides the ``archive`` sub-command group with operations to retrieve, list,
and download files stored in the content-addressed archive.
"""

from typing import Annotated

import typer
from anystore.io import smart_open

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import OPT_OUT, DatasetContext, console, sub_typer
from ftm_lakehouse.repository.factories import get_archive

archive = sub_typer("archive", "Access the file archive")


@archive.command("get")
def cli_archive_get(content_hash: str, out_uri: OPT_OUT = "-"):
    """Retrieve a file by content hash and write it to an output URI."""
    with DatasetContext() as (name, uri):
        with (
            get_archive(name, uri).open(content_hash) as i,
            smart_open(out_uri, "wb") as o,
        ):
            o.write(i.read())


@archive.command("ls")
def cli_archive_ls(out_uri: OPT_OUT = "-"):
    """List the checksums of all files in the dataset archive."""
    with DatasetContext() as (name, uri):
        checksums = get_archive(name, uri).iterate_checksums()
        with smart_open(out_uri, "wb") as o:
            o.writelines(f"{checksum}\n".encode() for checksum in checksums)


@archive.command("download")
def cli_archive_download(target: Annotated[str, typer.Option("-o")]):
    """Download all archive files to a local directory."""
    with DatasetContext() as (name, uri):
        res = op.download_archive(name, target, uri)
        console.print(res)
