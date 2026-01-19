# Working with Entities

The entities repository is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`. It provides a unified API for reading, writing, and querying entities.

## Overview

Entities in `ftm-lakehouse` are stored as **statements** - granular property-level records. This design enables:

- **Versioning**: Track changes over time
- **Provenance**: Know where each piece of data came from (via `origin` and the [Statement model](https://followthemoney.tech/docs/statements/))
- **Incremental updates**: Add new data without reprocessing everything
- **Deduplication**: Merge entities from multiple sources

The underlying storage is implemented with [parquet](https://parquet.apache.org/) files via [deltalake](https://delta-io.github.io/delta-rs/).

## Quick Start

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Write entities
with dataset.entities.bulk(origin="import") as writer:
    for entity in entities:
        writer.add_entity(entity)

# Read a specific entity
entity = dataset.entities.get("entity-id-123")

# Query entities
for entity in dataset.entities.query():
    process(entity)
```

Alternatively, use the shortcut to get the repository directly:

```python
from ftm_lakehouse import lake

entities = lake.get_entities("my_dataset")
entities.add(entity, origin="import")
```

## Writing Entities

### Single Entity

```python
from ftm_lakehouse import ensure_dataset
from followthemoney import model

dataset = ensure_dataset("my_dataset")

# Create an entity
entity = model.make_entity("Person")
entity.id = "jane-doe"
entity.add("name", "Jane Doe")
entity.add("nationality", "us")

# Write to the lakehouse
dataset.entities.add(entity, origin="manual")
```

### Bulk Writing

For large imports, use the bulk writer for better performance:

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Using the context manager
with dataset.entities.bulk(origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)
```

### Flush to Storage

Writes are buffered in a SQL journal. Flush to persist to Delta Lake:

```python
count = dataset.entities.flush()
print(f"Flushed {count} statements")
```

## Reading Entities

### Get by ID

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

entity = dataset.entities.get("jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Query by origin
for entity in dataset.entities.query(origin="import"):
    print(entity.id)

# Query specific entity IDs
ids = ["jane-doe", "john-smith"]
for entity in dataset.entities.query(entity_ids=ids):
    print(entity.caption)

# Query by schema bucket (thing, interval, address)
for entity in dataset.entities.query(bucket="thing"):
    print(entity.schema.name)
```

### Stream from Exported File

For full dataset iteration, streaming from the pre-exported JSON file is faster:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Stream from entities.ftm.json (requires prior export)
for entity in dataset.entities.stream():
    process(entity)
```

!!! note
    `stream()` reads from the exported `entities.ftm.json` file.
    Use `query()` to query the live statement store.

## The Origin Field

The `origin` field tracks where data came from. This is useful for:

- **Filtering**: Query only entities from a specific source
- **Auditing**: Know the provenance of each piece of data
- **Updates**: Replace data from one source without affecting others

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Import from different sources
with dataset.entities.bulk(origin="source_a") as writer:
    for entity in source_a_entities:
        writer.add_entity(entity)

with dataset.entities.bulk(origin="source_b") as writer:
    for entity in source_b_entities:
        writer.add_entity(entity)

# Query only source_a entities
for entity in dataset.entities.query(origin="source_a"):
    print(entity.id)
```

## Maintenance

### Flush the Journal

Writes are buffered in a SQL journal before being flushed to Delta Lake storage:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")
count = dataset.entities.flush()
print(f"Flushed {count} statements")
```

### Optimize Storage

Compact Delta Lake files for better read performance:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Optimize (compact files)
dataset.entities.optimize()

# Optimize and vacuum (remove old files)
dataset.entities.optimize(vacuum=True)
```

## Complete Example

Here's a complete example of an entity import pipeline:

```python
from ftm_lakehouse import ensure_dataset
from followthemoney import model


def create_person(name: str, nationality: str) -> model.EntityProxy:
    """Create a Person entity."""
    entity = model.make_entity("Person")
    entity.make_id(name)
    entity.add("name", name)
    entity.add("nationality", nationality)
    return entity


def main():
    # Ensure dataset exists
    dataset = ensure_dataset("people_dataset")

    # Create some entities
    people = [
        create_person("Jane Doe", "us"),
        create_person("John Smith", "gb"),
        create_person("Maria Garcia", "es"),
    ]

    # Write entities
    with dataset.entities.bulk(origin="manual") as writer:
        for person in people:
            writer.add_entity(person)

    # Flush to storage
    count = dataset.entities.flush()
    print(f"Flushed {count} statements")

    # Query back
    jane = dataset.entities.get(people[0].id)
    print(f"Found: {jane.caption}")

    # Query all
    print("All entities:")
    for entity in dataset.entities.query():
        print(f"  - {entity.caption}")


if __name__ == "__main__":
    main()
```
