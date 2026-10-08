# CLI Reference

`ftm-lakehouse` is a [Typer](https://typer.tiangolo.com/) CLI organised into sub-command groups.

```
ftm-lakehouse [OPTIONS] <group> <command> [ARGS]
```

| Group | Purpose |
|-------|---------|
| `archive` | Content-addressed file storage |
| `entities` | Read and write FtM entities |
| `statements` | Read and write raw FtM statements |
| `maintenance` | Storage maintenance (flush, optimize, shard, migrate, unlock) |
| `zfs` | ZFS dataset management |

Top-level commands: `ls` (dataset names), `datasets` (metadata), `configure` (write dataset configuration), `make` (build or update a dataset), `export` (write every export artifact), `crawl` (ingest documents into the archive).

Environment variables configure storage and behaviour – see the [configuration reference](../deployment/configuration.md).

## Examples

```bash
export LAKEHOUSE_URI=./data

# Initialise the dataset – no data yet, so flush only
ftm-lakehouse -d my_dataset make --no-exports

# Record its configuration (title, summary, shards, compression, ...)
ftm-lakehouse -d my_dataset configure -c config.yml

# Crawl some files
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Bulk-load a pre-built entities.ftm.json (skips the journal)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import

# Several times faster for trusted input – same statements, no FtM validation
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import --unsafe

# Flush the journal, optimize the store and build all exports – the default
ftm-lakehouse -d my_dataset make

# The export on its own – every artifact from one pass over the entities
ftm-lakehouse -d my_dataset export

# Drain the journal on its own – one dataset, or the whole catalog
ftm-lakehouse -d my_dataset maintenance flush
ftm-lakehouse maintenance flush --all

# Compact the store, on a schedule in production: merge each dirty
# (shard, bucket, origin) partition into one file, drop tombstones older than
# LAKEHOUSE_GRACE_PERIOD_DAYS, then vacuum the replaced files
ftm-lakehouse -d my_dataset maintenance optimize

# Change the shard count of an existing dataset – rewrites every partition.
# Run with writers stopped, then `maintenance optimize`
ftm-lakehouse -d my_dataset maintenance shard --shards 8

# Bring a store written by an older version up to date; `--all` sweeps the
# catalog (the docker entrypoint runs it). Run with writers stopped
ftm-lakehouse -d my_dataset maintenance migrate
ftm-lakehouse maintenance migrate --all
```

### `configure`

`ftm-lakehouse -d <dataset> configure -c <config.yml>` writes dataset configuration and nothing else. The yaml follows the [dataset configuration](../deployment/configuration.md#dataset-configuration) schema. Only the keys in the file are written, so a partial file leaves the rest (notably `shards`) untouched; `name` and `uri` come from `-d` / the catalog. Each write keeps a versioned snapshot.

Set `shards` before the dataset is written to. Changing it on a store that holds rows leaves the existing rows under the old count while reads prune by the new one, so `entity_id` lookups miss them. Use `maintenance shard --shards <n>` instead – see [Re-sharding](../architecture.md#re-sharding-an-existing-dataset).

### `make`

`make` runs the whole pipeline; every stage is on by default:

| Flag | Default | Effect |
|------|---------|--------|
| `-c <config.yml>` | – | Same merge-write as `configure`, before anything else |
| `--flush` / `--no-flush` | on | Flush outstanding journal statements into the parquet store |
| `--exports` / `--no-exports` | on | Write every export artifact – statements, entities, documents, parents, their diffs, the statistics and the index. `--no-exports` flushes only |
| `--optimize` / `--no-optimize` | on | Run [optimize](entities.md#maintenance) (merge + vacuum) before exporting; only with `--exports`. Exports are correct either way – an optimized store exports faster and takes less disk |
| `--force-optimize` | off | Optimize even when no partition is dirty |
| `--force-exports` | off | Export even when the tags say the exports are fresh |

### `maintenance flush`

`ftm-lakehouse -d <dataset> maintenance flush` drains the journal into the parquet store and prints how many statements landed – the first stage of `make`, on its own.

`--all` drains every dataset in the catalog, printing a count per dataset and the total; it can't be combined with `-d`. An empty journal is a cheap no-op, so `ftm-lakehouse maintenance flush --all` works as a cron entry. The first dataset that fails aborts the sweep.

## Commands

Generated from the CLI at docs build time:

{{ cli_docs() }}
