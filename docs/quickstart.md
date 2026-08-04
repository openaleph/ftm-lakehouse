# Quickstart

## Installation

Requires Python 3.11 or later.

```bash
pip install ftm-lakehouse
```

Remote storage backends (S3 / GCS / Azure / HTTP) are optional extras – see the [install notes](index.md#installation).

## Basic Concepts

`ftm-lakehouse` organizes data into **datasets**. Each dataset contains:

- **Entities**: Structured [FollowTheMoney](https://followthemoney.tech) data – [read more](./usage/entities.md)
- **Archive**: Source documents and files – [read more](./usage/archive.md)

## Using the CLI

The fastest way to a working dataset – point `LAKEHOUSE_URI` at a storage location and address datasets with `-d`:

```bash
export LAKEHOUSE_URI=./data

# Create (or update) a dataset
ftm-lakehouse -d my_dataset make

# Crawl source documents into the archive
ftm-lakehouse -d my_dataset operations crawl /path/to/documents

# Bulk-import FtM entities (bypasses the journal, writes directly to parquet)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import

# Build all exports: statements.csv, entities.ftm.json, statistics, index
ftm-lakehouse -d my_dataset make --full

# Stream entities back out
ftm-lakehouse -d my_dataset entities stream

# Maintenance – reads assume an optimized store, run after write batches
ftm-lakehouse -d my_dataset operations optimize
```

Every group and flag: [CLI Reference](usage/cli.md).

## Using the Python API

### Create a Dataset

```python
from ftm_lakehouse import ensure_dataset

# Get or create – config (shards, compression, metadata) is recorded at creation
ensure_dataset("my_dataset", title="My Dataset")
```

### Working with Entities

Repositories are the dataset handle – one per concern, addressed by name:

```python
from ftm_lakehouse import ensure_dataset, get_entities
from followthemoney import model

ensure_dataset("my_dataset")
entities = get_entities("my_dataset")

# Create an entity
person = model.make_entity("Person")
person.make_id("jane-doe")
person.add("name", "Jane Doe")
person.add("nationality", "us")

# Write the entity
entities.add(person, origin="manual")

# Flush to storage
entities.flush()

# Read it back
entity = entities.get(person.id)
print(f"Found: {entity.caption}")
```

### Working with Files

```python
from ftm_lakehouse import get_archive

archive = get_archive("my_dataset")

# Archive a file
file = archive.store("/path/to/document.pdf")
print(f"Archived: {file.checksum}")

# Retrieve it
with archive.open(file.checksum) as fh:
    content = fh.read()
```

### Bulk Operations

For large imports, use bulk writers:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Write many entities efficiently
with entities.writer(origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)

# Flush to parquet store
entities.flush()
```

### Query Entities

```python
# Query with filters
for entity in entities.query(origin="import"):
    print(entity.caption)

# Stream from exported JSON
for entity in entities.stream():
    print(entity.caption)
```

## Configuration

Set the storage location via environment variable:

```bash
# Local storage
export LAKEHOUSE_URI=./data

# S3 storage
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

For persistent journal storage (recommended for production):

```bash
export LAKEHOUSE_JOURNAL_URI=postgresql://user:pass@localhost/journal
```

Full settings reference: [Configuration](deployment/configuration.md).

## Next Steps

- [Working with Entities](usage/entities.md) - Deep dive into entity operations
- [Working with Files](usage/archive.md) - Learn about the file archive
- [CLI Reference](usage/cli.md) - Complete CLI documentation
- [Configuration](deployment/configuration.md) - Advanced configuration options
