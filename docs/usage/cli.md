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
| `maintenance` | Storage maintenance (flush, optimize, unlock) |
| `zfs` | ZFS dataset management |

Top-level (no group), as frequently-used shortcuts: `ls` (dataset names), `datasets` (metadata), `configure` (write dataset configuration), `make` (build/update a dataset), `export` (produce a single export kind), `crawl` (ingest documents into the archive).

Environment variables configure storage locations and behavior – see the [configuration reference](../deployment/configuration.md).

## Examples

```bash
export LAKEHOUSE_URI=./data

# Initialise the dataset – no data yet, so skip the exports pipeline
ftm-lakehouse -d my_dataset make --no-exports

# Record its configuration (title, summary, shards, compression, ...)
ftm-lakehouse -d my_dataset configure -c config.yml

# Crawl some files
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Bulk-load a pre-built entities.ftm.json (skips the journal)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import

# ... several times faster for trusted input (same statement ids and
# namespace stripping as the safe path, no FtM object construction):
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import --unsafe

# Flush the journal, optimize the store and build all exports – the default
ftm-lakehouse -d my_dataset make

# A single export kind on its own
ftm-lakehouse -d my_dataset export statistics

# Drain the journal on its own – one dataset, or the whole catalog
ftm-lakehouse -d my_dataset maintenance flush
ftm-lakehouse maintenance flush --all

# Maintenance – async, run on a schedule in production. Merges duplicates per
# (shard, bucket, origin) partition, drops tombstones older than
# LAKEHOUSE_GRACE_PERIOD_DAYS, bin-packs small files, removes obsolete ones –
# always in one pass, held under the dataset write fence.
ftm-lakehouse -d my_dataset maintenance optimize

# Change the shard count of an existing dataset: rewrites every partition,
# then records the new count in config.yml. Run with writers stopped, and
# follow up with `maintenance optimize`.
ftm-lakehouse -d my_dataset maintenance shard --shards 8
```

### `configure`

`ftm-lakehouse -d <dataset> configure -c <config.yml>` writes dataset configuration and nothing else – no flush, no exports. The yaml follows the [dataset configuration](../deployment/configuration.md#dataset-configuration) schema; only the keys it actually contains are written, so a partial file leaves everything else (notably `shards`) untouched. `name` and `uri` are taken from `-d` / the catalog and ignored if present in the file. Each write keeps a versioned snapshot.

Layout-affecting settings (`shards`) belong in the config *before* a dataset is written to. Setting a different value on a store that already holds rows splits it: rows written from then on are placed under the new count, the rows already there keep their old partitions, and reads prune by the new count – so an `entity_id`-filtered query silently misses whichever half didn't move. `maintenance shard --shards <n>` is the operation that moves them – see [Re-sharding](../architecture.md#re-sharding-an-existing-dataset).

### `make`

`make` is the whole pipeline in one command; every stage is on by default and can be switched off:

| Flag | Default | Effect |
|------|---------|--------|
| `-c <config.yml>` | – | Same merge-write as `configure`, before anything else |
| `--flush` / `--no-flush` | on | Flush outstanding journal statements into the parquet store |
| `--exports` / `--no-exports` | on | Build statements/entities/documents/statistics exports and diffs. With `--no-exports` only `index.json` is refreshed |
| `--optimize` / `--no-optimize` | on | Run the [optimize](entities.md#maintenance) pass before exporting (only applies with `--exports`) |
| `--force-optimize` | off | Optimize even when the store is already up-to-date |
| `--force-exports` | off | Re-compute the exports pipeline even when the tags say it is fresh |

### `maintenance flush`

`ftm-lakehouse -d <dataset> maintenance flush` drains outstanding journal statements into the parquet store and prints how many landed. It is the same drain `make` runs as its first stage, on its own – no optimize, no exports, so duplicates and tombstones stay as new rows until the next [optimize](entities.md#maintenance).

`--all` sweeps every dataset in the catalog instead, printing a count per dataset plus the total. It addresses the whole catalog, so combining it with `-d` is an error rather than a silent override. Datasets with an empty journal are a cheap no-op – the drain probes for rows before it rotates anything – which makes `ftm-lakehouse maintenance flush --all` a reasonable cron entry for a lakehouse whose writers leave data in the journal. It fails fast: the first dataset that errors aborts the sweep.

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
