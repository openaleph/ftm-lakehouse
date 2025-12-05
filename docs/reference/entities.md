# ftm_lakehouse.lake.entities

The entities module provides the unified interface for working with FollowTheMoney entities.

## DatasetEntities

The main interface for entity operations on a dataset:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

# Write entities
with entities.bulk(origin="import") as writer:
    writer.add_entity(entity)

# Read entities
entity = entities.get("entity-id")

# Query entities
for entity in entities.query(origin="import"):
    process(entity)

# Stream from exported file
for entity in entities.iterate():
    process(entity)
```

::: ftm_lakehouse.lake.entities.DatasetEntities
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - add
            - bulk
            - flush
            - get
            - query
            - iterate
            - export
            - export_statements
            - export_statistics
            - get_statistics
            - optimize
            - get_changed_statements

## JournalWriter

Bulk writer for adding entities to the journal:

```python
from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")

with entities.bulk(origin="import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

::: ftm_lakehouse.service.journal.JournalWriter
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - add_entity
            - add_statement
            - flush
            - rollback
