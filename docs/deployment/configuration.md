# Configuration

`ftm-lakehouse` can be configured via environment variables or YAML configuration files.

## Environment Variables

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_URI` | Base path to lakehouse storage | `./data` |
| `LAKEHOUSE_JOURNAL_URI` | SQLAlchemy URI for statement journal | `sqlite:///:memory:` |
| `LAKEHOUSE_GRACE_PERIOD_DAYS` | Default tombstone grace period used by `operations optimize` (rows with `deleted_at` older than this are physically dropped in the merge step) | `30` |
| `LAKEHOUSE_MAX_BUFFER_ROWS` | Hard cap on rows held in an in-memory `EntityBuffer` before a flush is required. Bulk-import paths that hit the cap raise `BufferFullError` and the caller flushes + retries. | `1_000_000` |
| `LAKEHOUSE_LOCK_MAX_RETRIES` | Retry bound when acquiring the dataset write fence (`.LOCK`). Retry `n` sleeps `n` + jitter seconds, so the total wait is roughly `N²/2` seconds; the default gives up after ~4.5 minutes with a `RuntimeError` instead of waiting forever. Stale locks from crashed writers need a manual `ftm-lakehouse operations unlock`. | `22` |
| `LAKEHOUSE_DUCKDB_MEMORY_LIMIT` | Per-DuckDB-connection RAM ceiling. Queries exceeding it spill to disk rather than growing toward all available RAM. Format follows DuckDB's `SET memory_limit` (e.g. `4GB`, `512MB`). | `4GB` |
| `LAKEHOUSE_DUCKDB_TEMP_DIRECTORY` | Spill-to-disk path for queries that overflow `LAKEHOUSE_DUCKDB_MEMORY_LIMIT`. Unset = DuckDB picks the OS temp dir. Point at a fast, capacity-controlled volume for heavy workloads. | (unset) |
| `LAKEHOUSE_ON_ZFS` | Enable ZFS dataset creation for local storage | `false` |
| `LAKEHOUSE_ZFS_POOL` | ZFS dataset path for the lakehouse root (e.g. `zpools/tank/lakehouse`) | (required when `ON_ZFS` is enabled) |
| `LAKEHOUSE_ZFS_SOCKET` | Unix socket path for remote ZFS operations (see [ZFS Integration](zfs.md)) | (unset) |
| `LAKEHOUSE_ZFS_OWNER` | `uid:gid` to chown new ZFS mountpoints to (see [ZFS Integration](zfs.md)) | (unset -- no chown) |
| `LAKEHOUSE_PUBLIC_URL_PREFIX` | Public URL prefix for blob URLs (supports `{{ dataset }}` Jinja-style template) | (unset) |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |
| `DEBUG` | Enable debug mode | `false` |

There is deliberately **no environment setting for the shard count**: it is per-dataset configuration (`shards` in `config.yml`, default `0` = single shard, immutable after first write), so a process with a different environment can't mis-shard an existing dataset. Huge datasets should configure `8` or more at creation – see [Sharding](../architecture.md#sharding-why-and-how-many-shards).

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

Each dataset can have its own `config.yml` file that follows the [ftmq.model.Dataset](https://github.com/dataresearchcenter/ftmq/blob/main/ftmq/model/dataset.py) specification:

```yaml
name: my_dataset  # also known as "foreign_id"
title: An Awesome Dataset
shards: 0  # entity-id hash shards; configure 8+ for huge datasets at creation
description: >
  A detailed description of this dataset,
  its sources, and contents.
updated_at: 2024-09-25
category: leak  # or: sanctions, pep, etc.
publisher:
  name: Data and Research Center – DARC
  url: https://dataresearchcenter.org
```

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

Requires extra install: `pip install gcsfs`

```bash
export LAKEHOUSE_URI=gs://bucket-name/prefix
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

### Azure Blob Storage

Requires extra install: `pip install adlfs`

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

The statement journal buffers writes before flushing to Delta Lake storage. For production use, configure a persistent database:

### SQLite (File-based)

```bash
export LAKEHOUSE_JOURNAL_URI=sqlite:///path/to/journal.db
```

### PostgreSQL

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

You can also configure programmatically:

```python
from ftm_lakehouse import get_lakehouse, get_dataset

# Get lakehouse with custom URI
lake = get_lakehouse(uri="s3://my-bucket/lakehouse")

# Get dataset
dataset = lake.get_dataset("my_dataset")
```

## Multi-Dataset Configuration

A lakehouse can contain multiple datasets, each with different configurations:

```
lakehouse/
  config.yml           # Catalog-level config
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

## Catalog Configuration

The lakehouse itself can have a `config.yml`:

```yaml
name: my-catalog
title: My Data Catalog
description: A collection of datasets
datasets:
  - name: dataset_a
  - name: dataset_b
```

The catalog `index.json` is automatically generated from dataset metadata.
