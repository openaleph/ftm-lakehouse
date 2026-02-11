"""Mapping management commands for the CLI.

Provides the ``mappings`` sub-command group for listing, inspecting, and
processing FtM mapping configurations stored in a dataset.
"""

from typing import Annotated, Optional

import typer
from anystore.io import smart_write
from anystore.util import dump_json_model

from ftm_lakehouse import operation as op
from ftm_lakehouse.cli import DatasetContext, cli, console, settings

mappings = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli.add_typer(mappings, name="mappings", help="Manage and process data mappings")


@mappings.command("ls")
def cli_mappings_ls(
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """List mapping configuration hashes in the dataset."""
    with DatasetContext() as dataset:
        hashes = list(dataset.mappings.list())
        smart_write(out_uri, "\n".join(hashes) + "\n" if hashes else "", "wb")


@mappings.command("get")
def cli_mappings_get(
    content_hash: str,
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """Retrieve a mapping configuration by its content hash."""
    with DatasetContext() as dataset:
        mapping = dataset.mappings.get(content_hash)
        if mapping is None:
            console.print(f"[red]No mapping found for {content_hash}[/red]")
            raise typer.Exit(code=1)
        smart_write(out_uri, dump_json_model(mapping, newline=True))


@mappings.command("process")
def cli_mappings_process(
    content_hash: Annotated[
        Optional[str], typer.Argument(help="Content hash to process (omit for all)")
    ] = None,
):
    """Process mapping configuration(s) and generate entities.

    If no content_hash is provided, processes all mappings in the dataset.
    """
    with DatasetContext() as dataset:
        if content_hash:
            result = op.run_mapping(dataset, content_hash)
            console.print(f"Generated {result.done} entities from {content_hash}")
        else:
            total = 0
            count = 0
            for mapping_hash in dataset.mappings.list():
                result = op.run_mapping(dataset, mapping_hash)
                if result.done > 0:
                    console.print(f"{mapping_hash}: {result.done} entities")
                total += result.done
                count += 1
            console.print(f"Total: {total} entities from {count} mappings")
