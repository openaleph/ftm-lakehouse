# CLI Reference

`ftm-lakehouse` provides a [Typer](https://typer.tiangolo.com/)-based command-line interface organised into sub-command groups.

```
ftm-lakehouse [OPTIONS] <group> <command> [ARGS]
```

| Group | Purpose |
|-------|---------|
| `archive` | Content-addressed file storage |
| `entities` | Read and write FtM entities |
| `statements` | Read and write raw FtM statements |
| `operations` | Dataset pipeline operations (export, optimize, unlock, crawl) |
| `zfs` | ZFS dataset management |

Top-level (no group): `ls` (dataset names), `datasets` (metadata), `make` (build/update a dataset – frequent shortcut, kept top-level).

Environment variables configure storage locations and behavior – see the [configuration reference](../deployment/configuration.md).

## Examples

```bash
export LAKEHOUSE_URI=./data

# Initialise the dataset
ftm-lakehouse -d my_dataset make

# Crawl some files
ftm-lakehouse -d my_dataset operations crawl /path/to/documents

# Bulk-load a pre-built entities.ftm.json (skips the journal)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import

# ... several times faster for trusted input (same statement ids and
# namespace stripping as the safe path, no FtM object construction):
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import --unsafe

# Build all exports
ftm-lakehouse -d my_dataset make --full

# Maintenance – async, run on a schedule in production. Merges duplicates per
# (shard, bucket, origin) partition, drops tombstones older than
# LAKEHOUSE_GRACE_PERIOD_DAYS, bin-packs small files, removes obsolete ones –
# always in one pass, held under the dataset write fence.
ftm-lakehouse -d my_dataset operations optimize
```

## Commands

The following reference is generated from the CLI itself at docs build time:

```python exec="on"
# In-process generation: `python -m typer <module> utils docs` loads the
# module standalone, so the sub-typer groups (registered by the trailing
# submodule imports) land on a second module instance and go missing.
import click
from typer import cli as typer_cli
from typer.main import get_command

from ftm_lakehouse.cli import cli as app

command = get_command(app)
with click.Context(command) as ctx:
    docs = typer_cli.get_docs_for_click(obj=command, ctx=ctx, name="ftm-lakehouse")

# demote headings one level so the generated tree nests under "## Commands"
for line in docs.splitlines():
    print("#" + line if line.startswith("#") else line)
```
