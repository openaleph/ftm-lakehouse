# Quickstart

## Installation

Requires Python 3.11 or later.

```bash
pip install ftm-lakehouse
```

Remote storage backends are optional extras – install the one matching your archive/lake URI:

```bash
pip install "ftm-lakehouse[s3]"     # S3-compatible object storage (s3fs)
pip install "ftm-lakehouse[gcs]"    # Google Cloud Storage (gcsfs)
pip install "ftm-lakehouse[azure]"  # Azure Blob Storage (adlfs)
pip install "ftm-lakehouse[http]"   # HTTP(S)-backed api store (aiohttp)
```

Extras combine, e.g. `pip install "ftm-lakehouse[s3,gcs]"`.

## Basic Concepts

`ftm-lakehouse` organizes data into **datasets**. Each dataset contains:

- **Entities**: Structured [FollowTheMoney](https://followthemoney.tech) data
- **Archive**: Source documents and files

## Using the Python API

### Get a Dataset

```python
from ftm_lakehouse import get_dataset, ensure_dataset

# Get existing dataset
dataset = get_dataset("my_dataset")

# Or create if it doesn't exist
dataset = ensure_dataset("my_dataset", title="My Dataset")
```

### Working with Entities

```python
from ftm_lakehouse import ensure_dataset
from followthemoney import model

dataset = ensure_dataset("my_dataset")

# Create an entity
person = model.make_entity("Person")
person.make_id("jane-doe")
person.add("name", "Jane Doe")
person.add("nationality", "us")

# Write the entity
dataset.get_entities().add(person, origin="manual")

# Flush to storage
dataset.get_entities().flush()

# Read it back
entity = dataset.get_entities().get(person.id)
print(f"Found: {entity.caption}")
```

### Working with Files

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Archive a file
file = dataset.get_archive().put("/path/to/document.pdf")
print(f"Archived: {file.checksum}")

# Retrieve it
with dataset.get_archive().open(file) as fh:
    content = fh.read()
```

### Bulk Operations

For large imports, use bulk writers:

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Write many entities efficiently
with dataset.get_entities().writer(origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)

# Flush to parquet store
dataset.get_entities().flush()
```

### Query Entities

```python
# Query with filters
for entity in dataset.get_entities().query(origin="import"):
    print(entity.caption)

# Stream from exported JSON
for entity in dataset.get_entities().stream():
    print(entity.caption)
```

## Using the CLI

### Create a Dataset

```bash
ftm-lakehouse -d my_dataset make
```

### Import Entities

```bash
# Bulk-import an FtM JSON file (bypasses the journal, writes directly to parquet)
cat entities.ftm.json | ftm-lakehouse -d my_dataset entities import
```

### Export Data

```bash
# Run the full pipeline: flush journal + all exports + index
ftm-lakehouse -d my_dataset make --full

# Stream pre-exported entities to stdout
ftm-lakehouse -d my_dataset entities stream

# Live read of the parquet store (no export file required)
ftm-lakehouse -d my_dataset entities iterate
```

### Crawl Documents

```bash
# Crawl from a local directory
ftm-lakehouse -d my_dataset operations crawl /path/to/documents

# Crawl from an HTTP source
ftm-lakehouse -d my_dataset operations crawl https://example.com/files/
```

### Work with Archive

```bash
# List archived files
ftm-lakehouse -d my_dataset archive ls

# Get file metadata
ftm-lakehouse -d my_dataset archive head <checksum>

# Retrieve file content
ftm-lakehouse -d my_dataset archive get <checksum> -o output.pdf
```

### Maintenance

The parquet statement store is append-only on the hot path. Deduplication and tombstone reaping happen via the async optimize operation (merge + compact + vacuum in one pass):

```bash
ftm-lakehouse -d my_dataset operations optimize
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

- [Usage Guide](usage.md) - Complete API usage guide
- [Working with Entities](usage/entities.md) - Deep dive into entity operations
- [Working with Files](usage/archive.md) - Learn about the file archive
- [CLI Reference](usage/cli.md) - Complete CLI documentation
- [Configuration](deployment/configuration.md) - Advanced configuration options
