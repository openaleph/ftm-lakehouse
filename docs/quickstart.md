# Quickstart

## Installation

Requires Python 3.12 or later.

```bash
pip install ftm-lakehouse
```

Remote storage backends, the postgres journal and the API server are optional extras – see the [install notes](index.md#installation).

## Basic Concepts

`ftm-lakehouse` organizes data into **datasets**. Each dataset contains:

- **Entities**: structured [FollowTheMoney](https://followthemoney.tech) data – [read more](./usage/entities.md)
- **Archive**: source documents and files – [read more](./usage/archive.md)

## Using the CLI

Point `LAKEHOUSE_URI` at a storage location and address datasets with `-d`:

```bash
export LAKEHOUSE_URI=./data

# Create the dataset – nothing to export yet
ftm-lakehouse -d my_dataset make --no-exports

# Crawl source documents into the archive
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Bulk-import FtM entities (bypasses the journal, writes directly to parquet)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import

# Flush the journal, optimize the store and write every export
ftm-lakehouse -d my_dataset make

# Stream entities back out
ftm-lakehouse -d my_dataset entities stream

# Compact the store – reads are correct without it, faster with it
ftm-lakehouse -d my_dataset maintenance optimize
```

Every group and flag: [CLI Reference](./usage/cli.md).

## Using the Python API

### Create a Dataset

```python
from ftm_lakehouse import ensure_dataset

# Get or create – config (shards, compression, metadata) is recorded at creation
ensure_dataset("my_dataset", title="My Dataset")
```

### Working with Entities

Repositories are the dataset handle, one per concern, addressed by name:

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

### Bulk Writing

For many entities, use a writer:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

with entities.writer(origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)

# Flush to parquet store
entities.flush()
```

### Query Entities

```python
from ftmq.query import C, Query

# Entities with a statement from this origin
for entity in entities.query(Query(C(origin="import"))):
    print(entity.caption)

# Stream the exported entities.ftm.json
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

The journal defaults to in-memory sqlite. In production, use postgres (needs the `postgres` extra):

```bash
export LAKEHOUSE_JOURNAL_URI=postgresql://user:pass@localhost/journal
```

Full settings reference: [Configuration](./deployment/configuration.md).

## Next Steps

- [Working with Entities](./usage/entities.md)
- [Working with Files](./usage/archive.md)
- [CLI Reference](./usage/cli.md)
- [Configuration](./deployment/configuration.md)
