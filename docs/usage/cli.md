# CLI Reference

`ftm-lakehouse` provides a [Typer](https://typer.tiangolo.com/)-based command-line interface organised into sub-command groups.

```
ftm-lakehouse [OPTIONS] <group> <command> [ARGS]
```

Groups:

| Group | Purpose |
|-------|---------|
| `archive` | Content-addressed file storage |
| `entities` | Read and write FtM entities |
| `statements` | Read and write raw FtM statements |
| `mappings` | CSV-to-entity mapping configurations |
| `operations` | Dataset pipeline operations (export, compact, merge, vacuum, crawl) |
| `zfs` | ZFS dataset management |

Top-level (no group):

| Command | Purpose |
|---------|---------|
| `ls` | List dataset names in the catalog |
| `datasets` | Show metadata for all datasets in the catalog |
| `make` | Make/update a dataset (frequent shortcut, kept top-level) |

## Global Options

| Option | Description |
|--------|-------------|
| `--version` | Show version |
| `--settings` | Print current settings |
| `--uri` | Lakehouse URI (overrides `LAKEHOUSE_URI`) |
| `-d, --dataset` | Dataset name (required for most commands) |

## Top-level

### `ls`

```bash
# List dataset names
ftm-lakehouse ls

# Output to file
ftm-lakehouse ls -o datasets.txt
```

### `datasets`

```bash
ftm-lakehouse datasets
ftm-lakehouse datasets -o datasets.jsonl
```

### `make`

```bash
# Build or refresh a dataset (flush journal + ensure index.json)
ftm-lakehouse -d my_dataset make

# Full pipeline: flush + all exports
ftm-lakehouse -d my_dataset make --full

# Re-run even when freshness tags say it's up-to-date
ftm-lakehouse -d my_dataset make --full --force

# Apply a config.yml override
ftm-lakehouse -d my_dataset make -c path/to/config.yml
```

## `entities`

Read/write FtM entities (aggregated, statement-aware).

```bash
ftm-lakehouse entities --help
```

| Command | Purpose |
|---------|---------|
| `iterate` | Live read from the parquet store as FtM JSON lines |
| `stream` | Stream the pre-exported `entities.ftm.json` to stdout |
| `import` | Bulk import FtM JSON entities straight into the parquet store (bypasses the journal) |

```bash
# Live read of the parquet store
ftm-lakehouse -d my_dataset entities iterate
ftm-lakehouse -d my_dataset entities iterate -o entities.live.json

# Stream the frozen export (entities.ftm.json must exist)
ftm-lakehouse -d my_dataset entities stream
ftm-lakehouse -d my_dataset entities stream -o out.json

# Bulk import (the file is shard-sorted in memory then written directly to
# parquet – journal is bypassed for one-shot loads)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import
ftm-lakehouse -d my_dataset entities import -i entities.ftm.json --origin bulk
ftm-lakehouse -d my_dataset entities import -i entities.ftm.json --bulk-size 250000
```

## `statements`

Raw statement-grain read/write, mirroring `entities` at the lower level.

| Command | Purpose |
|---------|---------|
| `iterate` | Live read from the parquet store as CSV rows |
| `stream` | Stream the pre-exported `statements.csv` to stdout |
| `import` | Bulk import statements (CSV) straight into the parquet store |

```bash
ftm-lakehouse -d my_dataset statements iterate -o live-statements.csv
ftm-lakehouse -d my_dataset statements stream -o exported.csv
cat statements.csv | ftm-lakehouse -d my_dataset statements import
```

## `operations`

Pipeline operations on a dataset.

| Command | Purpose |
|---------|---------|
| `export-statements` | Export the parquet store → `exports/statements.csv` |
| `export-entities` | Export → `entities.ftm.json` |
| `export-statistics` | Export → `exports/statistics.json` |
| `export-documents` | Export → `exports/documents.csv` |
| `compact` | Bin-pack small parquet files (cheap) |
| `merge` | Collapse duplicates, reap expired tombstones (expensive) |
| `vacuum` | Delete obsolete parquet files no longer referenced by the Delta log |
| `crawl` | Crawl documents from a local/remote source into the archive |

### Exports

```bash
ftm-lakehouse -d my_dataset operations export-statements
ftm-lakehouse -d my_dataset operations export-entities
ftm-lakehouse -d my_dataset operations export-statistics
ftm-lakehouse -d my_dataset operations export-documents
```

### Maintenance (async, on the parquet statement store)

```bash
# Cheap file bin-pack – does not change row contents.
ftm-lakehouse -d my_dataset operations compact

# Collapse duplicates per (shard, bucket, origin) partition; drop tombstones
# older than `LAKEHOUSE_GRACE_PERIOD_DAYS`.
ftm-lakehouse -d my_dataset operations merge

# Remove obsolete parquet files (the ones merge/compact have tombstoned).
ftm-lakehouse -d my_dataset operations vacuum
ftm-lakehouse -d my_dataset operations vacuum --retention-hours 24
```

All three acquire a dataset-wide write fence at `.LOCK`, so they don't race with each other or with append-style writes.

### Crawl

```bash
ftm-lakehouse -d my_dataset operations crawl /path/to/documents
ftm-lakehouse -d my_dataset operations crawl https://example.com/files/
ftm-lakehouse -d my_dataset operations crawl /path --include "*.pdf"
ftm-lakehouse -d my_dataset operations crawl /path --exclude "*.tmp"
```

## `archive`

```bash
# List archived files
ftm-lakehouse -d my_dataset archive ls
ftm-lakehouse -d my_dataset archive ls --keys       # paths only
ftm-lakehouse -d my_dataset archive ls --checksums  # checksums only

# Inspect / fetch
ftm-lakehouse -d my_dataset archive head <checksum>
ftm-lakehouse -d my_dataset archive get  <checksum> -o document.pdf

# Bulk download to a local mirror
ftm-lakehouse -d my_dataset archive download -o /tmp/mirror
```

## `mappings`

```bash
# Discover mappings
ftm-lakehouse -d my_dataset mappings ls
ftm-lakehouse -d my_dataset mappings get <content_hash>

# Process one mapping, or all mappings in the dataset
ftm-lakehouse -d my_dataset mappings process <content_hash>
ftm-lakehouse -d my_dataset mappings process
```

## `zfs`

```bash
# Host-side socket agent (creates ZFS datasets on behalf of containerised clients)
ftm-lakehouse zfs agent --socket /run/zfs.sock --pool zpools/tank/lakehouse

# Manual init of a dataset's ZFS hierarchy
ftm-lakehouse zfs init my_dataset --pool zpools/tank/lakehouse
```

The `zfs` group does not require a catalog (it's about provisioning, not data).

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_URI` | Base path to lakehouse storage | `data` |
| `LAKEHOUSE_JOURNAL_URI` | SQLAlchemy URI for the journal | `sqlite:///:memory:` |
| `LAKEHOUSE_ENTITY_SHARDS` | Uniform shard count per new dataset | `8` |
| `LAKEHOUSE_GRACE_PERIOD_DAYS` | Tombstone grace period used by `operations merge` | `30` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `DEBUG` | Pretty-print tracebacks etc. | `false` |

See also [Configuration](../deployment/configuration.md) for storage backend options (S3, GCS, Azure) and [ZFS Integration](../deployment/zfs.md).

## Examples

### End-to-end ingestion

```bash
export LAKEHOUSE_URI=./my_lakehouse

# Initialise the dataset
ftm-lakehouse -d my_dataset make

# Crawl some files
ftm-lakehouse -d my_dataset operations crawl /path/to/documents

# Bulk-load a pre-built entities.ftm.json (skips the journal)
ftm-lakehouse -d my_dataset entities import -i entities.ftm.json

# Build all exports
ftm-lakehouse -d my_dataset make --full

# Maintenance – async, run on a schedule in production
ftm-lakehouse -d my_dataset operations compact
ftm-lakehouse -d my_dataset operations merge
ftm-lakehouse -d my_dataset operations vacuum
```

### S3-backed storage

```bash
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

ftm-lakehouse ls
ftm-lakehouse -d my_dataset make --full
```
