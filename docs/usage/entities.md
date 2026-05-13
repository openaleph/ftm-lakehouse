# Working with Entities

The entities repository is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`. It provides a unified API for reading, writing, and querying entities.

## Overview

Entities in `ftm-lakehouse` are stored as **statements** – granular property-level records. This design enables:

- **Versioning**: Track changes over time via `first_seen` / `last_seen`
- **Provenance**: Know where each piece of data came from (`origin`, `original_value`, and other metadata from the [Statement model](https://followthemoney.tech/docs/statements/))
- **Incremental updates**: Add new data without reprocessing everything
- **Deduplication**: Merge entities from multiple sources via `canonical_id`

The underlying storage is a single Delta Lake table per dataset, partitioned by `(shard, bucket, origin)`:

- `shard` – `hash(entity_id) % LAKEHOUSE_ENTITY_SHARDS`, hex-padded
- `bucket` – coarse FtM schema group (`thing`, `interval`, `document`, `page`, `pages`, `mention`)
- `origin` – caller-supplied source tag

Writes are **append-only**. Deduplication, `first_seen` folding, and tombstone reaping happen via three async maintenance operations (`compact`, `merge`, `vacuum`), all guarded by a dataset-wide write fence at `.LOCK`.

## Quick Start

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Write entities
with dataset.entities.writer(origin="import") as writer:
    for entity in entities:
        writer.add_entity(entity)

# Persist the journal to parquet
dataset.entities.flush()

# Read a specific entity
entity = dataset.entities.get("entity-id-123")

# Query entities
for entity in dataset.entities.query():
    process(entity)
```

The repository shortcut:

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

entity = model.make_entity("Person")
entity.id = "jane-doe"
entity.add("name", "Jane Doe")
entity.add("nationality", "us")

dataset.entities.add(entity, origin="manual")
```

### Bulk Writing (through the journal)

For interactive ingestion that wants the journal's per-row dedup and crash-safety guarantees:

```python
with dataset.entities.writer(origin="bulk_import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

Writes buffer in a SQL journal. Call `dataset.entities.flush()` to drain the journal into parquet:

```python
count = dataset.entities.flush()
print(f"Flushed {count} statements")
```

### Bulk Import (bypassing the journal)

For one-shot loads where journal write-amplification is wasteful (millions of entities from an exported file), stream through an in-memory shard buffer and write directly to parquet:

```python
from datetime import datetime, timezone
from ftmq.io import smart_read_proxies
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

repo = dataset.entities
buffer = EntityBuffer(dataset.name, dataset.model.shards, origin="bulk")
now = datetime.now(timezone.utc)

for proxy in smart_read_proxies("entities.ftm.json"):
    buffer.add_entity(proxy)
    if len(buffer) >= 1_000_000:
        repo.write_statements(buffer.flush_buffer(), now=now)

if buffer:
    repo.write_statements(buffer.flush_buffer(), now=now)
```

The `EntityBuffer` keys statements by ID and sorts by shard on flush; `repo.write_statements` packs the sorted stream per partition into one parquet file per `(shard, bucket, origin)` triple.

The CLI command `ftm-lakehouse entities import` does exactly this.

## Reading Entities

### Get by ID

```python
entity = dataset.entities.get("jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

```python
for entity in dataset.entities.query(origin="import"):
    print(entity.id)

ids = ["jane-doe", "john-smith"]
for entity in dataset.entities.query(entity_ids=ids):
    print(entity.caption)

# By schema bucket (thing / interval / document / page / pages / mention)
for entity in dataset.entities.query(bucket="thing"):
    print(entity.schema.name)
```

### Stream from Exported File

For full-dataset iteration, streaming from the pre-exported JSON file is typically faster than running an aggregating query against the parquet store:

```python
for entity in dataset.entities.stream():
    process(entity)
```

`stream()` reads from `entities.ftm.json`. Use `query()` to read the live statement store; `iterate()` on the CLI does the same.

## The Origin Field

`origin` is part of the partition key (alongside `shard` and `bucket`) and tracks where data came from. Useful for filtering, auditing, and partition-scoped re-runs:

```python
with dataset.entities.writer(origin="source_a") as writer:
    for entity in source_a_entities:
        writer.add_entity(entity)

with dataset.entities.writer(origin="source_b") as writer:
    for entity in source_b_entities:
        writer.add_entity(entity)

for entity in dataset.entities.query(origin="source_a"):
    print(entity.id)
```

## Deleting Entities

Deletes are tombstones routed through the journal (or `EntityBuffer` for the bulk path). They land in parquet as rows with `deleted_at` set. The default query view filters out tombstones via `deleted_at IS NULL`, so deleted entities disappear from `query()` and `stream()` as soon as `merge` has collapsed the live + tombstone pair.

### Delete an Entity

```python
count = dataset.entities.delete_entity("jane-doe")
print(f"Wrote {count} tombstones")

dataset.entities.flush()
dataset.entities.merge()  # collapse live+tombstone → tombstone survives until grace
```

### Delete a Single Statement

```python
stmts = list(dataset.entities.query_statements())
target = stmts[0]

dataset.entities.delete_statement(target)
dataset.entities.flush()
dataset.entities.merge()
```

### Re-adding After Delete

```python
dataset.entities.delete_entity("jane-doe")
dataset.entities.flush()
dataset.entities.merge(grace_period_days=0)  # drop tombstones immediately

dataset.entities.add(updated_jane, origin="correction")
dataset.entities.flush()
# jane-doe is alive again with the new data
```

## Deduplication

**On write**: identical statements within the same journal window are de-duplicated by primary key (`id`); the journal's `ON CONFLICT (id) DO UPDATE` collapses re-emissions.

**Across flushes**: re-flushing the same statement appends a new parquet row. The duplicates only collapse when `merge` runs. `merge` keeps the row with the latest `last_seen` per statement id and folds `first_seen` to the minimum across the group.

```python
dataset.entities.add(entity)
dataset.entities.flush()   # one row in parquet
dataset.entities.add(entity)
dataset.entities.flush()   # two rows now; same statement.id

dataset.entities.merge()   # back to one row, last_seen=now, first_seen=original
```

If you want immediate-effect dedup at write time, use the journal path with `flush()` – the journal upsert dedups within a window – and run `merge` on a schedule.

## Maintenance

Three independent async operations on the parquet statement store. All three acquire a dataset-wide write fence at `.LOCK`, so they don't race with each other or with appends from `flush` / `write_statements`.

### Flush (journal → parquet)

```python
count = dataset.entities.flush()
```

Drains the journal in one shard-sorted pass; each per-shard batch becomes one parquet file per `(shard, bucket, origin)` partition. No dedup happens here – duplicates and tombstones land as new rows for `merge` to collapse later.

### Compact (cheap)

Bin-packs small parquet files within each `(shard, bucket, origin)` partition via Delta's `OPTIMIZE compact`. Does not change row contents.

```python
dataset.entities._statements.compact()
```

### Merge (expensive)

Per-partition rewrite that collapses duplicates (`ROW_NUMBER OVER (PARTITION BY id ORDER BY last_seen DESC) = 1`), folds `first_seen` to the min across the id group, and drops tombstones whose `deleted_at` is older than the grace cutoff.

```python
dataset.entities.merge()  # uses default grace from settings
dataset.entities.merge(grace_period_days=0)  # drop all tombstones immediately
```

Default grace is `LAKEHOUSE_GRACE_PERIOD_DAYS` (30 days).

### Vacuum

Deletes obsolete parquet files that `merge` / `compact` have tombstoned in the Delta log.

```python
dataset.entities._statements.vacuum()
dataset.entities._statements.vacuum(retention_hours=24)
```

## Complete Example

```python
from ftm_lakehouse import ensure_dataset
from followthemoney import model


def create_person(name: str, nationality: str) -> model.EntityProxy:
    entity = model.make_entity("Person")
    entity.make_id(name)
    entity.add("name", name)
    entity.add("nationality", nationality)
    return entity


def main():
    dataset = ensure_dataset("people_dataset")

    people = [
        create_person("Jane Doe", "us"),
        create_person("John Smith", "gb"),
        create_person("Maria Garcia", "es"),
    ]

    # Write
    with dataset.entities.writer(origin="manual") as writer:
        for person in people:
            writer.add_entity(person)
    count = dataset.entities.flush()
    print(f"Flushed {count} statements")

    # Maintenance – run on a schedule in production
    dataset.entities._statements.compact()
    dataset.entities.merge()

    # Read back
    jane = dataset.entities.get(people[0].id)
    print(f"Found: {jane.caption}")

    for entity in dataset.entities.query():
        print(f"  - {entity.caption}")


if __name__ == "__main__":
    main()
```
