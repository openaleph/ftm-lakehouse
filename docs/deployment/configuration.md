# Configuration

`ftm-lakehouse` is configured via environment variables and a per-dataset `config.yml`.

## Environment Variables

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_URI` | Base path or URI of the lakehouse storage | `data` |
| `LAKEHOUSE_JOURNAL_URI` | SQLAlchemy URI of the statement journal – see [Journal Database](#journal-database) | `sqlite:///:memory:` |
| `LAKEHOUSE_GRACE_PERIOD_DAYS` | Days a tombstone (`deleted_at`) is kept before the merge of `maintenance optimize` drops it and the rows it shadows | `30` |
| `LAKEHOUSE_MAX_BUFFER_ROWS` | Row cap of an in-memory `EntityBuffer`. Bulk imports that hit it raise `BufferFullError`; the caller flushes and retries. | `1_000_000` |
| `LAKEHOUSE_JOURNAL_DRAIN_ROWS` | Rows per Arrow table a journal flush hands to the parquet store – one parquet file per partition the table spans. Higher means fewer, bigger files; lower it if a flush runs out of memory, as each table is held whole while it is written. | `1_000_000` |
| `LAKEHOUSE_JOURNAL_POOL_SIZE` | Postgres journal connections kept idle between writers, one pool per journal uri and process, shared by all datasets. Writers beyond it open their own connection rather than queue. Multiply by the process count to size against postgres `max_connections`; `0` pools nothing. | `5` |
| `LAKEHOUSE_LOCK_MAX_RETRIES` | Retry bound for every wait on a dataset lock – acquiring `.LOCK` or `.LOCK-MERGE`, and appends backing off while `.LOCK` is held. The total wait is about `N²/2` seconds (a minute at the default), then `RuntimeError`. A lock left by a crashed process needs `ftm-lakehouse maintenance unlock`. | `10` |
| `LAKEHOUSE_DUCKDB_MEMORY_LIMIT` | DuckDB's memory budget; queries beyond it spill to disk. A byte size (`8GB`, `512MiB`) – a share such as `80%` fails at startup, since the limit is split between the workers – see [Sizing parallel workers](#sizing-parallel-workers). | `8GB` |
| `LAKEHOUSE_WORKERS` | Processes the merge of `maintenance optimize` and the export sweep spread their partitions over; `1` (the minimum) runs in-process. The memory limit and CPU threads are split between them – see [Sizing parallel workers](#sizing-parallel-workers). An export uses at most one worker per `(shard, bucket)` pair (`shards` × at most five buckets), and `TMPDIR` holds the parts of the pairs in flight. | `1` |
| `LAKEHOUSE_DUCKDB_TEMP_DIRECTORY` | Where DuckDB spills queries that outgrow the memory limit, each DuckDB instance into its own subdirectory, removed on close (a killed process leaves it behind). Point it at a fast volume with room – not a `tmpfs` `/tmp`, which spills into RAM. Empty uses DuckDB's own default, `.tmp` in the working directory. | `{OS temp dir}/duckdb` |
| `LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY` | Where DuckDB loads (and auto-installs) extensions. Unset means `$HOME/.duckdb/extensions`, which fails without a writable `HOME`. The Docker image pre-installs `delta` into `/opt/duckdb/extensions` and sets this. | (unset) |
| `LAKEHOUSE_ON_ZFS` | Create tuned ZFS datasets for new datasets – see [ZFS Integration](zfs.md) | `false` |
| `LAKEHOUSE_ZFS_POOL` | ZFS dataset path new datasets are created under (e.g. `zpools/tank/lakehouse`). Transport settings (`ZFS_SOCKET`, `ZFS_OWNER`, …) belong to the [zfs-agent](https://github.com/dataresearchcenter/zfs-agent) package. | (required with `LAKEHOUSE_ON_ZFS`) |
| `LAKEHOUSE_API_KEY` / `LAKEHOUSE_API_SECRET` | Sent as `X-Api-Key` / `X-Api-Secret` on a client's lakehouse-API requests, for the reverse proxy in front of the server to check | (unset) |
| `LAKEHOUSE_PUBLIC_URL_PREFIX` | Public URL prefix for blob URLs, `${dataset}` substituted. A dataset's own `public_url_prefix` takes precedence. | (unset) |
| `LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO` |
| `DEBUG` | Enable debug mode | `false` |

There is no environment setting for the shard count: `shards` is per-dataset configuration in `config.yml` (default `0`, a single shard), set at creation – `8` or more for huge datasets. Changing it later rewrites every partition (`maintenance shard`) – see [Sharding](../architecture.md#sharding-why-and-how-many-shards).

### Sizing parallel workers

Each worker's DuckDB gets `LAKEHOUSE_DUCKDB_MEMORY_LIMIT / LAKEHOUSE_WORKERS`, in a merge and in the export sweep alike. Two rules follow:

- **The share has to cover the heaviest partition.** Sorts and windows spill, but the full-text values of the `document` and `page` buckets are held in memory, so a share that is too small fails the partition with DuckDB's `Out of Memory Error`. The defaults fail this way on full-text partitions: `8GB` split 8 ways is 1GB a worker. A merge needs a few times a partition's *uncompressed* size, which its size on disk understates.
- **RAM has to fit `workers × (share + ~2GB)`, plus the ARC on ZFS.** A worker peaks 1–2GB above its share (DuckDB allocations outside the limit, its Python process). When the sum does not fit, the kernel kills a worker and the merge stops with `BrokenProcessPool`; finished partitions are committed, so a rerun picks up the rest.

When the share is too small, lower `LAKEHOUSE_WORKERS` rather than the share. For reference, a 140GB (zstd) store of ~500 partitions merges at 80–120MB/s with `LAKEHOUSE_WORKERS=8` and `LAKEHOUSE_DUCKDB_MEMORY_LIMIT=128GB` on 48 cores and 256GB RAM.

The largest partitions on disk (after a `vacuum`, since unreferenced files count too):

```bash
du -sh "$LAKEHOUSE_URI/<dataset>/statements"/shard=*/bucket=*/origin=* | sort -h | tail
```

Workers are spawned processes that re-import the main module. The CLI is safe; a script calling `merge` or `export` with more than one worker needs an `if __name__ == "__main__":` guard, and cannot be piped into `python` on stdin.

### Basic Usage

```bash
# Local filesystem
export LAKEHOUSE_URI=./my_lakehouse

# S3 storage
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# With persistent journal (for production)
export LAKEHOUSE_JOURNAL_URI=postgresql://user:pass@localhost/journal
```

## Dataset Configuration

Each dataset has its own `config.yml`, following the [ftmq.model.Dataset](https://github.com/dataresearchcenter/ftmq/blob/main/ftmq/model/dataset.py) specification:

```yaml
name: my_dataset  # also known as "foreign_id"
title: An Awesome Dataset
shards: 0  # entity-id hash shards; configure 8+ for huge datasets at creation
compression: zst  # compress exported artifacts (gz / zst; unset = uncompressed)
description: >
  A detailed description of this dataset,
  its sources, and contents.
updated_at: 2024-09-25
category: leak  # or: sanctions, pep, etc.
publisher:
  name: Data and Research Center – DARC
  url: https://dataresearchcenter.org
```

Write it with `ftm-lakehouse -d my_dataset configure -c config.yml` (or `update_dataset()` from Python). Both merge – keys absent from the yaml keep their current value – and keep a versioned snapshot of each write.

## Storage Backends

### Local Filesystem

```bash
export LAKEHOUSE_URI=/path/to/lakehouse
```

### Amazon S3

```bash
export LAKEHOUSE_URI=s3://bucket-name/prefix
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1
```

### S3-Compatible (MinIO, etc.)

```bash
export LAKEHOUSE_URI=s3://bucket-name/prefix
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_ENDPOINT_URL=https://minlake.example.com
```

### Google Cloud Storage

Requires the `gcs` extra: `pip install "ftm-lakehouse[gcs]"`

```bash
export LAKEHOUSE_URI=gs://bucket-name/prefix
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

### Azure Blob Storage

Requires the `azure` extra: `pip install "ftm-lakehouse[azure]"`

```bash
export LAKEHOUSE_URI=az://container-name/prefix
export AZURE_STORAGE_ACCOUNT_NAME=your_account
export AZURE_STORAGE_ACCOUNT_KEY=your_key
```

Or using connection string:

```bash
export LAKEHOUSE_URI=az://container-name/prefix
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

Or using SAS token:

```bash
export LAKEHOUSE_URI=az://container-name/prefix
export AZURE_STORAGE_ACCOUNT_NAME=your_account
export AZURE_STORAGE_SAS_TOKEN="?sv=2021-06-08&ss=b&srt=sco&sp=rwdlacyx..."
```

Or using Azure AD / Service Principal:

```bash
export LAKEHOUSE_URI=az://container-name/prefix
export AZURE_STORAGE_ACCOUNT_NAME=your_account
export AZURE_STORAGE_TENANT_ID=your_tenant_id
export AZURE_STORAGE_CLIENT_ID=your_client_id
export AZURE_STORAGE_CLIENT_SECRET=your_client_secret
```

## Journal Database

The statement journal buffers writes until they are flushed into the parquet store. Use a persistent database in production.

### SQLite (File-based)

```bash
export LAKEHOUSE_JOURNAL_URI=sqlite:///path/to/journal.db
```

### PostgreSQL

Requires the `postgres` extra (`pip install "ftm-lakehouse[postgres]"`); without it, creating the journal fails.

```bash
export LAKEHOUSE_JOURNAL_URI=postgresql://user:password@host:5432/database
```

### In-Memory (for debugging / testing)

```bash
export LAKEHOUSE_JOURNAL_URI=sqlite:///:memory:
```

!!! warning
    The in-memory journal is lost when the process exits. Use a persistent database for production workloads.

## Python Configuration

```python
from ftm_lakehouse import get_entities, get_lakehouse

# Get lakehouse with custom URI
lake = get_lakehouse(uri="s3://my-bucket/lakehouse")

# Repositories per dataset (uri derived from the catalog)
entities = get_entities("my_dataset", lake.dataset_uri("my_dataset"))
```

## Multi-Dataset Configuration

A lakehouse holds multiple datasets, each with its own configuration:

```
lakehouse/
  dataset_a/
    config.yml         # Dataset A config
    archive/
    ...
  dataset_b/
    config.yml         # Dataset B config (could point to remote storage)
    ...
```

A dataset can reference remote storage while appearing in a local catalog:

```yaml
# lakehouse/remote_dataset/config.yml
name: remote_dataset
title: Remote Dataset
# This dataset's data lives in S3
storage:
  uri: s3://remote-bucket/dataset
```

## Catalog

The catalog is the storage root itself – any directory under the lakehouse uri that contains a `config.yml` is a dataset. `get_lakehouse().list_datasets()` enumerates them; there is no catalog-level configuration file.
