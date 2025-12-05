# Configuration

`ftm-lakehouse` can be configured via environment variables or YAML configuration files.

## Environment Variables

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_URI` | Base path to lakehouse storage | `./data` |
| `LAKEHOUSE_JOURNAL_URI` | SQLAlchemy URI for statement journal | `sqlite:///:memory:` |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |
| `DEBUG` | Enable debug mode | `false` |

### Basic Usage

```bash
# Local filesystem
export LAKEHOUSE_URI=./my_lakehouse

# S3 storage
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# With persistent journal (recommended for production)
export LAKEHOUSE_JOURNAL_URI=postgresql://user:pass@localhost/journal
```

## Dataset Configuration

Each dataset can have its own `config.yml` file that follows the [ftmq.model.Dataset](https://github.com/dataresearchcenter/ftmq/blob/main/ftmq/model/dataset.py) specification:

```yaml
name: my_dataset  # also known as "foreign_id"
title: An Awesome Dataset
description: >
  A detailed description of this dataset,
  its sources, and contents.
updated_at: 2024-09-25
category: leak  # or: sanctions, pep, etc.
publisher:
  name: Data Research Center
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
export AWS_ENDPOINT_URL=https://minio.example.com
```

### Google Cloud Storage

```bash
export LAKEHOUSE_URI=gs://bucket-name/prefix
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
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
    The in-memory journal is lost when the process exits.
    Use a persistent database for production workloads.

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
