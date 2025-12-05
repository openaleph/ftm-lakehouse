# Working with Entities

The entities interface is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`. It provides a unified API for reading, writing, and querying entities.

## Overview

Entities in `ftm-lakehouse` are stored as **statements** - granular property-level records. This design enables:

- **Versioning**: Track changes over time
- **Provenance**: Know where each piece of data came from (via `origin` and the [Statement model](https://followthemoney.tech/docs/statements/))
- **Incremental updates**: Add new data without reprocessing everything
- **Deduplication**: Merge entities from multiple sources

The underlying storage is implemented with a [parquet](https://parquet.apache.org/) files [deltalake](https://delta-io.github.io/delta-rs/).

## Quick Start

```python
from ftm_lakehouse import io

# Get or create a dataset
dataset = io.ensure_dataset("my_dataset")

# Write entities
io.write_entities("my_dataset", entities, origin="import")

# Read a specific entity
entity = io.get_entity("my_dataset", "entity-id-123")

# Stream all entities
for entity in io.stream_entities("my_dataset"):
    process(entity)
```

## Writing Entities

### Single Entity

```python
from ftm_lakehouse import io
from followthemoney import model

# Create an entity
entity = model.make_entity("Person")
entity.id = "jane-doe"
entity.add("name", "Jane Doe")
entity.add("nationality", "us")

# Write to the lakehouse
io.write_entity("my_dataset", entity, origin="manual")
```

### Bulk Writing

For large imports, use the bulk writer for better performance:

```python
from ftm_lakehouse import io

# Using the context manager
with io.entity_writer("my_dataset", origin="bulk_import") as writer:
    for entity in large_entity_source():
        writer.add_entity(entity)

# Or use the convenience function
count = io.write_entities("my_dataset", entities, origin="import")
print(f"Wrote {count} entities")
```

### Using the Entities Interface

You can also work directly with the entities interface:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Add a single entity
entities.add(entity, origin="api")

# Bulk write
with entities.bulk(origin="import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)

# Flush pending writes to storage
entities.flush()
```

## Reading Entities

### Get by ID

```python
from ftm_lakehouse import io

# Get a specific entity
entity = io.get_entity("my_dataset", "jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

```python
from ftm_lakehouse import io

# Query by origin
for entity in io.iterate_entities("my_dataset", origin="import"):
    print(entity.id)

# Query specific entity IDs
ids = ["jane-doe", "john-smith"]
for entity in io.iterate_entities("my_dataset", entity_ids=ids):
    print(entity.caption)

# Query by schema bucket (thing, interval, address)
for entity in io.iterate_entities("my_dataset", bucket="thing"):
    print(entity.schema.name)
```

### Stream from Exported File

For full dataset iteration, streaming from the pre-exported JSON file is faster:

```python
from ftm_lakehouse import io

# Stream from entities.ftm.json (requires prior export)
for entity in io.stream_entities("my_dataset"):
    process(entity)
```

!!! note
    `stream_entities` reads from the exported `entities.ftm.json` file.
    Use `iterate_entities` to query the live statement store.

## The Origin Field

The `origin` field tracks where data came from. This is useful for:

- **Filtering**: Query only entities from a specific source
- **Auditing**: Know the provenance of each piece of data
- **Updates**: Replace data from one source without affecting others

```python
# Import from different sources
io.write_entities("my_dataset", source_a_entities, origin="source_a")
io.write_entities("my_dataset", source_b_entities, origin="source_b")

# Query only source_a entities
for entity in io.iterate_entities("my_dataset", origin="source_a"):
    print(entity.id)
```

## Exporting Data

### Export to JSON

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Export statements to CSV (required before entity export)
entities.export_statements()

# Export entities to JSON
entities.export()
```

### Export Statistics

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Compute and export statistics
entities.export_statistics()

# Get statistics
stats = entities.get_statistics()
print(f"Entity count: {stats.entity_count}")
```

### Full Dataset Update

The `make()` method runs a complete update pipeline:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Run full update:
# 1. Flush journal to statement store
# 2. Export statements.csv
# 3. Export statistics.json
# 4. Export entities.ftm.json
# 5. Update index.json
dataset.make()
```

## Maintenance

### Flush the Journal

Writes are buffered in a SQL journal before being flushed to Delta Lake storage:

```python
from ftm_lakehouse import io, get_entities

# Using io module
io.flush("my_dataset")

# Or via entities interface
entities = get_entities("my_dataset")
count = entities.flush()
print(f"Flushed {count} statements")
```

### Optimize Storage

Compact Delta Lake files for better read performance:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Optimize (compact files)
entities.optimize()

# Optimize and vacuum (remove old files)
entities.optimize(vacuum=True)
```

## Complete Example

Here's a complete example of an entity import pipeline:

```python
from ftm_lakehouse import io, get_entities
from followthemoney import model

def create_person(name: str, nationality: str) -> model.EntityProxy:
    """Create a Person entity."""
    entity = model.make_entity("Person")
    entity.make_id(name)
    entity.add("name", name)
    entity.add("nationality", nationality)
    return entity

def main():
    dataset_name = "people_dataset"

    # Ensure dataset exists
    io.ensure_dataset(dataset_name)

    # Create some entities
    people = [
        create_person("Jane Doe", "us"),
        create_person("John Smith", "gb"),
        create_person("Maria Garcia", "es"),
    ]

    # Write entities
    count = io.write_entities(dataset_name, people, origin="manual")
    print(f"Wrote {count} entities")

    # Flush and export
    entities = get_entities(dataset_name)
    entities.flush()
    entities.export_statements()
    entities.export()

    # Query back
    jane = io.get_entity(dataset_name, people[0].id)
    print(f"Found: {jane.caption}")

    # Stream all
    print("All entities:")
    for entity in io.stream_entities(dataset_name):
        print(f"  - {entity.caption}")

if __name__ == "__main__":
    main()
```
