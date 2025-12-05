# Quickstart

This guide will help you get started with `ftm-lakehouse` in minutes.

## Installation

Requires Python 3.11 or later.

```bash
pip install ftm-lakehouse
```

## Basic Concepts

`ftm-lakehouse` organizes data into **datasets**. Each dataset contains:

- **Entities**: Structured [FollowTheMoney](https://followthemoney.tech) data
- **Archive**: Source documents and files

## Using the Python API

### Working with Entities

```python
from ftm_lakehouse import io
from followthemoney import model

# Create a dataset
dataset = io.ensure_dataset("my_dataset")

# Create an entity
person = model.make_entity("Person")
person.make_id("jane-doe")
person.add("name", "Jane Doe")
person.add("nationality", "us")

# Write the entity
io.write_entity("my_dataset", person, origin="manual")

# Flush to storage
io.flush("my_dataset")

# Read it back
entity = io.get_entity("my_dataset", person.id)
print(f"Found: {entity.caption}")
```

### Working with Files

```python
from ftm_lakehouse import io

# Archive a file
file = io.archive_file("my_dataset", "/path/to/document.pdf")
print(f"Archived: {file.checksum}")

# Retrieve it
with io.open_file("my_dataset", file.checksum) as fh:
    content = fh.read()
```

### Bulk Operations

For large imports, use bulk writers:

```python
from ftm_lakehouse import io

# Write many entities efficiently
with io.entity_writer("my_dataset", origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)

# Flush and export
dataset = io.ensure_dataset("my_dataset")
dataset.entities.flush()
dataset.entities.export_statements()
dataset.entities.export()
```

## Using the CLI

### Create a Dataset

```bash
# Initialize a dataset
ftm-lakehouse -d my_dataset make
```

### Crawl Documents

```bash
# Crawl from a local directory
ftm-lakehouse -d my_dataset crawl /path/to/documents

# Crawl from HTTP source
ftm-lakehouse -d my_dataset crawl https://example.com/files/
```

### Import Entities

```bash
# Import from JSON lines file
cat entities.ftm.json | ftm-lakehouse -d my_dataset write-entities
```

### Export Data

```bash
# Generate all exports
ftm-lakehouse -d my_dataset make --exports

# Stream entities
ftm-lakehouse -d my_dataset stream-entities
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

## Complete Example

Here's a complete workflow:

```python
from ftm_lakehouse import io, get_dataset
from followthemoney import model

def main():
    dataset_name = "quickstart_demo"

    # 1. Create or get the dataset
    dataset = io.ensure_dataset(dataset_name)

    # 2. Create some entities
    entities = []
    for name in ["Alice Smith", "Bob Jones", "Carol White"]:
        person = model.make_entity("Person")
        person.make_id(name.lower().replace(" ", "-"))
        person.add("name", name)
        entities.append(person)

    # 3. Write entities
    count = io.write_entities(dataset_name, entities, origin="demo")
    print(f"Wrote {count} entities")

    # 4. Flush and export
    dataset.entities.flush()
    dataset.entities.export_statements()
    dataset.entities.export()

    # 5. Query entities
    print("\nAll entities:")
    for entity in io.stream_entities(dataset_name):
        print(f"  - {entity.caption}")

    # 6. Get specific entity
    alice = io.get_entity(dataset_name, "alice-smith")
    print(f"\nFound Alice: {alice.caption}")

if __name__ == "__main__":
    main()
```

## Next Steps

- [Working with Entities](usage/entities.md) - Deep dive into entity operations
- [Working with Files](usage/archive.md) - Learn about the file archive
- [CLI Reference](usage/cli.md) - Complete CLI documentation
- [Configuration](usage/configuration.md) - Advanced configuration options
