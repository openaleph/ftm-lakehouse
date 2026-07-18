# Working with Entities

The entities repository is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`. It provides a unified API for reading, writing, and querying entities.

## Overview

Entities in `ftm-lakehouse` are stored as **statements** – granular property-level records. This design enables:

- **Versioning**: Track changes over time via `first_seen` / `last_seen`
- **Provenance**: Know where each piece of data came from (`origin`, `original_value`, and other metadata from the [Statement model](https://followthemoney.tech/docs/statements/))
- **Incremental updates**: Add new data without reprocessing everything
- **Deduplication**: Merge entities from multiple sources via `canonical_id`

The underlying storage is a single Delta Lake table per dataset, partitioned by `(shard, bucket, origin)`:

- `shard` – `hash(entity_id) % shards` (the dataset's configured shard count, default a single shard), hex-padded
- `bucket` – coarse FtM schema group (`thing`, `interval`, `document`, `page`, `pages`, `mention`)
- `origin` – caller-supplied source tag

Writes are **append-only**. Deduplication, `first_seen` folding, and tombstone reaping happen via the async `optimize` operation (merge + compact + vacuum), guarded by a dataset-wide write fence at `.LOCK`.

## Quick Start

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Write entities
with dataset.get_entities().writer(origin="import") as writer:
    for entity in entities:
        writer.add_entity(entity)

# Persist the journal to parquet
dataset.get_entities().flush()

# Read a specific entity
entity = dataset.get_entities().get("entity-id-123")

# Query entities
for entity in dataset.get_entities().query():
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

dataset.get_entities().add(entity, origin="manual")
```

### Bulk Writing (through the journal)

For interactive ingestion that wants the journal's per-row dedup and crash-safety guarantees:

```python
with dataset.get_entities().writer(origin="bulk_import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

Writes buffer in a SQL journal. Call `dataset.get_entities().flush()` to drain the journal into parquet:

```python
count = dataset.get_entities().flush()
print(f"Flushed {count} statements")
```

### Bulk Import (bypassing the journal)

For one-shot loads where journal write-amplification is wasteful (millions of entities from an exported file), stream through an in-memory shard buffer and write directly to parquet:

```python
from datetime import datetime, timezone
from ftmq.io import smart_read_proxies
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

repo = dataset.get_entities()
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
entity = dataset.get_entities().get("jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

```python
for entity in dataset.get_entities().query(origin="import"):
    print(entity.id)

ids = ["jane-doe", "john-smith"]
for entity in dataset.get_entities().query(entity_ids=ids):
    print(entity.caption)

# By schema bucket (thing / interval / document / page / pages / mention)
for entity in dataset.get_entities().query(bucket="thing"):
    print(entity.schema.name)
```

### Stream from Exported File

For full-dataset iteration, streaming from the pre-exported JSON file is typically faster than running an aggregating query against the parquet store:

```python
for entity in dataset.get_entities().stream():
    process(entity)
```

`stream()` reads from `entities.ftm.json`. Use `query()` to read the live statement store; `iterate()` on the CLI does the same.

## The Origin Field

`origin` is part of the partition key (alongside `shard` and `bucket`) and tracks where data came from. Useful for filtering, auditing, and partition-scoped re-runs:

```python
with dataset.get_entities().writer(origin="source_a") as writer:
    for entity in source_a_entities:
        writer.add_entity(entity)

with dataset.get_entities().writer(origin="source_b") as writer:
    for entity in source_b_entities:
        writer.add_entity(entity)

for entity in dataset.get_entities().query(origin="source_a"):
    print(entity.id)
```

## Fragment Supersession

Every statement is written in one of two modes, decided by the producer per statement. The default is **non-fragment**: content-addressed dedup, where each statement `id` lives or dies on its own `last_seen` and distinct ids never interact – everything described in this document so far.

Passing a `fragment` switches a statement into **supersession** mode (the same capability as the original [followthemoney-store](https://github.com/alephdata/followthemoney-store) `fragment` column): a later emission of the same `(entity_id, prop, fragment)` triple completely replaces the older emission for that triple, even though the changed values produce different content-addressed statement ids.

```python
with dataset.get_entities().writer(origin="csv_import") as writer:
    writer.add_entity(company, fragment="row42")

# later, the source row changed – re-emit under the same fragment:
with dataset.get_entities().writer(origin="csv_import") as writer:
    writer.add_entity(updated_company, fragment="row42")

# after flush, only the updated values are visible – the first emission
# is superseded, not accumulated
```

The typical use is one fragment per source row in a CSV-style ingest (or per document in a crawler): re-processing the source replaces what that row previously said about the entity instead of accumulating stale values forever. `add_statement` accepts the same parameter for statement-level producers.

### Semantics

- **Scope is per `(entity_id, prop, fragment)`**, not per fragment as a whole. If the first emission had `name`, `address` and `country` and the re-emission only has `name` and `address`, the old `country` value survives – no newer row exists in its group. If you want whole-fragment replacement, emit explicit tombstones for the dropped props: statements read back from the store are `ftmq.store.lake.LakeStatement`s carrying their own fragment, so `delete_statement(stmt)` shadows the right group; the `fragment=` override is only needed for hand-built plain statements.
- **Multi-valued props survive together.** All rows of one emission share a `last_seen`, so all values of the latest emission are kept (ties at the group maximum), and all values of older emissions go.
- **The two modes are isolated.** A non-fragment row never supersedes a fragment row or vice versa, even with identical content. The same statement can legitimately exist under multiple fragments (and additionally without one) – `fragment` is part of the journal's primary key, but not part of the statement `id`.
- **Origins are isolated too.** The same fragment written under two different origins forms two independent supersession groups, matching the `(shard, bucket, origin)` partition scope of `merge`.
- **Tombstones participate.** A tombstone written with the fragment supersedes its group like any emission; the group disappears from queries immediately and is physically reaped once the tombstone passes the grace period. `delete_entity` handles this automatically – it reads each live row's fragment and writes fragment-matched tombstones.

### Producer contract

All rows of one logical fragment emission **must share the same `last_seen` timestamp** – supersession keeps every row tied at the group's maximum, so jitter within an emission would keep only the very latest row and break multi-valued props. `add_entity` pins one timestamp per emission (from the entity's `last_seen` / `last_change`, falling back to a single `now`); statement-level producers assign one timestamp per batch themselves:

```python
ts = datetime.now(timezone.utc).isoformat()
with dataset.get_entities().writer(origin="import") as writer:
    for prop, value in row_values:
        writer.add_statement(
            Statement(entity_id=entity_id, prop=prop, value=value, schema=schema, dataset=dataset.name, last_seen=ts),
            fragment=f"row{row_number}",
        )
```

Note that the FtM statement model truncates `last_seen` to **second granularity**: two emissions of the same fragment within the same second tie, and both survive. Distinct emissions need distinct timestamps – re-processing loops faster than once per second should carry producer-assigned timestamps.

In storage, "no fragment" is the empty string, never NULL; the SDK translates `fragment=None` to `''` at the boundary.

## Deleting Entities

Deletes are tombstones routed through the journal (or `EntityBuffer` for the bulk path). They land in parquet as rows with `deleted_at` set. The default query view filters out tombstones via `deleted_at IS NULL`, so deleted entities disappear from `query()` and `stream()` as soon as `merge` has collapsed the live + tombstone pair.

### Delete an Entity

```python
count = dataset.get_entities().delete_entity("jane-doe")
print(f"Wrote {count} tombstones")

dataset.get_entities().flush()
dataset.get_entities().merge()  # collapse live+tombstone → tombstone survives until grace
```

### Delete a Single Statement

```python
stmts = list(dataset.get_entities().query_statements())
target = stmts[0]

dataset.get_entities().delete_statement(target)
dataset.get_entities().flush()
dataset.get_entities().merge()
```

### Re-adding After Delete

```python
dataset.get_entities().delete_entity("jane-doe")
dataset.get_entities().flush()
dataset.get_entities().merge(grace_period_days=0)  # drop tombstones immediately

dataset.get_entities().add(updated_jane, origin="correction")
dataset.get_entities().flush()
# jane-doe is alive again with the new data
```

## Deduplication

**On write**: identical statements within the same journal window are de-duplicated by primary key (`id, fragment`); the journal's `ON CONFLICT (id, fragment) DO UPDATE` collapses re-emissions.

**Across flushes**: re-flushing the same statement appends a new parquet row. The duplicates only collapse when `merge` runs. `merge` keeps the row with the latest `last_seen` per statement id (per supersession group for fragment rows) and folds `first_seen` to the minimum across the group.

```python
dataset.get_entities().add(entity)
dataset.get_entities().flush()   # one row in parquet
dataset.get_entities().add(entity)
dataset.get_entities().flush()   # two rows now; same statement.id

dataset.get_entities().merge()   # back to one row, last_seen=now, first_seen=original
```

If you want immediate-effect dedup at write time, use the journal path with `flush()` – the journal upsert dedups within a window – and run `merge` on a schedule.

## Maintenance

Three independent async operations on the parquet statement store. All three acquire a dataset-wide write fence at `.LOCK`, so they don't race with each other or with appends from `flush` / `write_statements`.

### Flush (journal → parquet)

```python
count = dataset.get_entities().flush()
```

Drains the journal in one shard-sorted pass; each per-shard batch becomes one parquet file per `(shard, bucket, origin)` partition. No dedup happens here – duplicates and tombstones land as new rows for `merge` to collapse later.

### Compact (cheap)

Bin-packs small parquet files within each `(shard, bucket, origin)` partition via Delta's `OPTIMIZE compact`. Does not change row contents.

```python
dataset.get_entities()._statements.compact()
```

### Merge (expensive)

Per-partition rewrite that collapses duplicates, folds `first_seen` to the min across each group, and drops tombstones whose `deleted_at` is older than the grace cutoff. Non-fragment rows dedupe per statement `id` (`ROW_NUMBER OVER (PARTITION BY id ORDER BY last_seen DESC) = 1`); fragment rows keep the latest emission per `(entity_id, prop, fragment)` group.

```python
dataset.get_entities().merge()  # uses default grace from settings
dataset.get_entities().merge(grace_period_days=0)  # drop all tombstones immediately
```

Default grace is `LAKEHOUSE_GRACE_PERIOD_DAYS` (30 days).

### Vacuum

Deletes obsolete parquet files that `merge` / `compact` have tombstoned in the Delta log.

```python
dataset.get_entities()._statements.vacuum()
dataset.get_entities()._statements.vacuum(retention_hours=24)
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
    with dataset.get_entities().writer(origin="manual") as writer:
        for person in people:
            writer.add_entity(person)
    count = dataset.get_entities().flush()
    print(f"Flushed {count} statements")

    # Maintenance – run on a schedule in production
    dataset.get_entities()._statements.compact()
    dataset.get_entities().merge()

    # Read back
    jane = dataset.get_entities().get(people[0].id)
    print(f"Found: {jane.caption}")

    for entity in dataset.get_entities().query():
        print(f"  - {entity.caption}")


if __name__ == "__main__":
    main()
```
