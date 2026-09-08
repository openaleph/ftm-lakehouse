"""Zensical `macros` module: build-time values for the documentation.

Registered as ``plugins.macros.module_name`` in ``mkdocs.yml``. The macros
extension is a Python-Markdown *preprocessor* – jinja renders the page source
before it is parsed – so whatever a macro returns is ordinary Markdown: its
headings land in the tree natively and reach the table of contents like any
hand-written ones.

It deliberately lives outside ``docs/``: everything in the docs directory is
copied into the built site.
"""

from typing import Any

import click
from typer import cli as typer_cli
from typer.main import get_command

from ftm_lakehouse.cli import cli as app


def define_env(env: Any) -> None:
    """Register the macros. Called by the `macros` extension on startup."""

    @env.macro
    def cli_docs(depth: int = 2) -> str:
        """Render the whole `ftm-lakehouse` CLI as Markdown.

        In-process generation: ``python -m typer <module> utils docs`` loads
        the module standalone, so the sub-typer groups (registered by the
        trailing submodule imports) land on a second module instance and go
        missing.

        Args:
            depth: Heading level the root command renders at, so the generated
                tree nests under the section that calls the macro.

        Returns:
            Markdown for the command tree, headings demoted by ``depth - 1``.
        """
        command = get_command(app)
        with click.Context(command) as ctx:
            docs = typer_cli.get_docs_for_click(
                obj=command, ctx=ctx, name="ftm-lakehouse"
            )
        prefix = "#" * (depth - 1)
        return "\n".join(
            prefix + line if line.startswith("#") else line
            for line in docs.splitlines()
        )
