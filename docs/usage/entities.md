# Working with Entities

The entities repository is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`. It provides a unified API for reading, writing, and querying entities.

## Overview

Entities in `ftm-lakehouse` are stored as **statements** – granular property-level records. This design enables:

- **Versioning**: Track changes over time via `first_seen` / `last_seen`
- **Provenance**: Know where each piece of data came from (`origin`, `original_value`, and other metadata from the [Statement model](https://followthemoney.tech/docs/statements/))
- **Incremental updates**: Add new data without reprocessing everything
- **Simple identity**: entities are keyed on `entity_id`; this is a single-dataset store with no cross-source resolution, so `canonical_id` is not persisted (it always equals `entity_id`)

The underlying storage is a single Delta Lake table per dataset, partitioned by `(shard, bucket, origin)` – see [Sharded append-only pattern](../architecture.md#sharded-append-only-pattern) for the partition keys, write fence and merge semantics. Writes are **append-only**: deduplication, `first_seen` folding, and tombstone reaping happen via the async `optimize` operation.

## Quick Start

```python
from ftm_lakehouse import ensure_dataset, get_entities

ensure_dataset("my_dataset")
entities = get_entities("my_dataset")

# Write entities
with entities.writer(origin="import") as writer:
    for entity in source:
        writer.add_entity(entity)

# Persist the journal to parquet
entities.flush()

# Read a specific entity
entity = entities.get("entity-id-123")

# Query entities
for entity in entities.query():
    process(entity)
```

The `EntityRepository` handle is resolved through an LRU-cached factory – every path addressing the same dataset (library, CLI, operations, API server) shares one instance.

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

entities.add(entity, origin="manual")
```

### Bulk Writing (through the journal)

For interactive ingestion that wants the journal's crash-safety guarantees:

```python
with entities.writer(origin="bulk_import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

Writes buffer in a SQL journal – an append-only table carrying the same columns as the parquet store. Call `entities.flush()` to drain it into parquet:

```python
count = entities.flush()
print(f"Flushed {count} statements")
```

### Bulk Import (bypassing the journal)

For one-shot loads where journal write-amplification is wasteful (millions of entities from an exported file), stream through an in-memory shard buffer and write directly to parquet:

```python
from datetime import datetime, timezone
from ftmq.io import smart_read_proxies
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

repo = get_entities("my_dataset")
buffer = EntityBuffer(repo.dataset, repo.shards, origin="bulk")
now = datetime.now(timezone.utc)

for proxy in smart_read_proxies("entities.ftm.json"):
    buffer.add_entity(proxy)
    if len(buffer) >= 1_000_000:
        repo.write_batches(buffer.flush_tables(now))

if buffer:
    repo.write_batches(buffer.flush_tables(now))
```

The `EntityBuffer` keys statements by `(id, origin, fragment)` and holds them grouped by shard; `buffer.flush_tables()` drains it as one packed Arrow table per shard, and `repo.write_batches` appends each one as a parquet file per `(shard, bucket, origin)` triple.

The CLI command `ftm-lakehouse entities import` does exactly this.

## Reading Entities

!!! note "Reads assume an optimized store"
    Statement reads target a live `WHERE deleted_at IS NULL` view with no read-time dedupe. Dedupe, fragment supersession, and tombstone reaping all happen in `merge` (see [Deduplication](#deduplication)), so between a write and the next `optimize`/`merge`, `query` can surface duplicate statements and entities whose delete hasn't been applied yet. Run `optimize` before querying, exporting, or computing statistics.

### Get by ID

```python
entity = entities.get("jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

Filters are expressed as an [ftmq `Query`](https://docs.investigraph.dev/lib/ftmq/query) – built from filter nodes (`M` for statement meta fields like `origin` / `entity_id` / `schema`, `P` for entity properties):

```python
from ftmq.query import M, Query

for entity in entities.query(Query(M(origin="import"))):
    print(entity.id)

ids = ["jane-doe", "john-smith"]
for entity in entities.query(Query(M(entity_id__in=ids))):
    print(entity.caption)

# By schema – the (shard, bucket) partition prunes are derived from the
# query (schema → bucket, entity_id → shard)
for entity in entities.query(Query(M(schema="Person"))):
    print(entity.schema.name)
```

### Stream from Exported File

For full-dataset iteration, streaming from the pre-exported JSON file is typically faster than running an aggregating query against the parquet store:

```python
for entity in entities.stream():
    process(entity)
```

`stream()` reads from `entities.ftm.json`. Use `query()` to read the live statement store; `iterate()` on the CLI does the same.

## The Origin Field

`origin` is part of the partition key (alongside `shard` and `bucket`) and tracks where data came from. Useful for filtering, auditing, and partition-scoped re-runs:

```python
with entities.writer(origin="source_a") as writer:
    for entity in source_a_entities:
        writer.add_entity(entity)

with entities.writer(origin="source_b") as writer:
    for entity in source_b_entities:
        writer.add_entity(entity)

for entity in entities.query(Query(M(origin="source_a"))):
    print(entity.id)
```

## Fragment Supersession

Every statement is written in one of two modes, decided by the producer per statement. The default is **non-fragment**: content-addressed dedup, where each statement `id` lives or dies on its own `last_seen` and distinct ids never interact – everything described in this document so far.

Passing a `fragment` switches a statement into **supersession** mode (the same capability as the original [followthemoney-store](https://github.com/alephdata/followthemoney-store) `fragment` column): a later emission of the same `(entity_id, prop, fragment)` triple completely replaces the older emission for that triple, even though the changed values produce different content-addressed statement ids.

```python
with entities.writer(origin="csv_import") as writer:
    writer.add_entity(company, fragment="row42")

# later, the source row changed – re-emit under the same fragment:
with entities.writer(origin="csv_import") as writer:
    writer.add_entity(updated_company, fragment="row42")

# after flush, only the updated values are visible – the first emission
# is superseded, not accumulated
```

The typical use is one fragment per source row in a CSV-style ingest (or per document in a crawler): re-processing the source replaces what that row previously said about the entity instead of accumulating stale values forever. `add_statement` accepts the same parameter for statement-level producers.

### Semantics

- **Scope is per `(entity_id, prop, fragment)`**, not per fragment as a whole. If the first emission had `name`, `address` and `country` and the re-emission only has `name` and `address`, the old `country` value survives – no newer row exists in its group. If you want whole-fragment replacement, emit explicit tombstones for the dropped props: statements read back from the store are `ftmq.store.lake.LakeStatement`s carrying their own fragment, so `delete_statement(stmt)` shadows the right group; the `fragment=` override is only needed for hand-built plain statements.
- **Multi-valued props survive together.** All rows of one emission share a `last_seen`, so all values of the latest emission are kept (ties at the group maximum), and all values of older emissions go.
- **The two modes are isolated.** A non-fragment row never supersedes a fragment row or vice versa, even with identical content. The same statement can legitimately exist under multiple fragments (and additionally without one) – `fragment` is part of the stored row identity, but not part of the statement `id`.
- **Origins are isolated too.** The same fragment written under two different origins forms two independent supersession groups, matching the `(shard, bucket, origin)` partition scope of `merge`.
- **Tombstones participate.** A tombstone written with the fragment supersedes its group like any emission; the group disappears from queries immediately and is physically reaped once the tombstone passes the grace period. `delete_entity` handles this automatically – it reads each live row's fragment and writes fragment-matched tombstones.

### Producer contract

All rows of one logical fragment emission **must share the same `last_seen` timestamp** – supersession keeps every row tied at the group's maximum, so jitter within an emission would keep only the very latest row and break multi-valued props. `add_entity` pins one timestamp per fragment emission (from the entity's `last_seen` / `last_change`, falling back to a single `now`); non-fragment emissions keep each statement's own `last_seen` (faithful provenance on store round-trips) and only fall back to the pinned value when unset. Statement-level producers assign one timestamp per batch themselves:

```python
ts = datetime.now(timezone.utc).isoformat()
with entities.writer(origin="import") as writer:
    for prop, value in row_values:
        writer.add_statement(
            Statement(entity_id=entity_id, prop=prop, value=value, schema=schema, dataset="my_dataset", last_seen=ts),
            fragment=f"row{row_number}",
        )
```

Note that the FtM statement model truncates `last_seen` to **second granularity**: two emissions of the same fragment within the same second tie, and both survive. Distinct emissions need distinct timestamps – re-processing loops faster than once per second should carry producer-assigned timestamps.

In storage, "no fragment" is the empty string, never NULL; the SDK translates `fragment=None` to `''` at the boundary.

## Deleting Entities

Deletes are tombstones routed through the journal (or `EntityBuffer` for the bulk path). They land in parquet as rows with `deleted_at` set. The default query view filters out tombstones via `deleted_at IS NULL`, so deleted entities disappear from `query()` and `stream()` as soon as `merge` has collapsed the live + tombstone pair.

### Delete an Entity

```python
count = entities.delete_entity("jane-doe")
print(f"Wrote {count} tombstones")

entities.flush()
entities.merge()  # collapse live+tombstone → tombstone survives until grace
```

### Delete a Single Statement

```python
stmts = list(entities.query_statements())
target = stmts[0]

entities.delete_statement(target)
entities.flush()
entities.merge()
```

### Re-adding After Delete

```python
entities.delete_entity("jane-doe")
entities.flush()
entities.merge()  # set LAKEHOUSE_GRACE_PERIOD_DAYS=0 to drop tombstones immediately

entities.add(updated_jane, origin="correction")
entities.flush()
# jane-doe is alive again with the new data
```

## Deduplication

**On write**: identical statements collapse only inside one writer batch, where the in-memory buffer keys rows by `(id, fragment, origin)`. The journal itself is append-only and keyless – re-emissions accumulate as rows.

**Across flushes**: re-flushing the same statement appends a new parquet row. The duplicates only collapse when `merge` runs. `merge` keeps the row with the latest `last_seen` per statement id (per supersession group for fragment rows) and folds `first_seen` to the minimum across the group.

```python
entities.add(entity)
entities.flush()   # one row in parquet
entities.add(entity)
entities.flush()   # two rows now; same statement.id

entities.merge()   # back to one row, last_seen=now, first_seen=original
```

Dedup is `merge`'s job alone – there is no write-time collapse to lean on, so run `merge` on a schedule (or via `optimize`) and treat queries as accurate on an optimized store.

## Maintenance

Three independent async operations on the parquet statement store, held under the exclusive [maintenance fence](../architecture.md#sharded-append-only-pattern) so they never race each other or in-flight appends.

### Flush (journal → parquet)

```python
count = entities.flush()
```

Claims the journal by rotating it away, then streams the rotated segment into parquet as Arrow batches, shard-ordered; each batch becomes one parquet file per `(shard, bucket, origin)` partition. Writers keep going against the fresh journal table throughout. No dedup happens here – duplicates and tombstones land as new rows for `merge` to collapse later.

From the CLI, per dataset or across the whole catalog:

```bash
ftm-lakehouse -d my_dataset maintenance flush
ftm-lakehouse maintenance flush --all
```

### Compact (cheap)

Bin-packs small parquet files within each `(shard, bucket, origin)` partition via Delta's `OPTIMIZE compact`. Does not change row contents.

```python
entities._statements.compact()
```

### Merge (expensive)

Per-partition rewrite that collapses duplicates, folds `first_seen` to the min across each group, and drops tombstones whose `deleted_at` is older than the grace cutoff. Non-fragment rows dedupe per statement `id` (`ROW_NUMBER OVER (PARTITION BY id ORDER BY last_seen DESC) = 1`); fragment rows keep the latest emission per `(entity_id, prop, fragment)` group.

```python
entities.merge()
```

Grace comes from `LAKEHOUSE_GRACE_PERIOD_DAYS` (default 30 days); set it to `0` to drop all tombstones immediately.

### Vacuum

Deletes obsolete parquet files that `merge` / `compact` have tombstoned in the Delta log.

```python
entities._statements.vacuum()
entities._statements.vacuum(retention_hours=24)
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
    with entities.writer(origin="manual") as writer:
        for person in people:
            writer.add_entity(person)
    count = entities.flush()
    print(f"Flushed {count} statements")

    # Maintenance – run on a schedule in production
    entities._statements.compact()
    entities.merge()

    # Read back
    jane = entities.get(people[0].id)
    print(f"Found: {jane.caption}")

    for entity in entities.query():
        print(f"  - {entity.caption}")


if __name__ == "__main__":
    main()
```

## Multiple Datasets

The catalog enumerates all datasets under one storage root:

```python
from ftm_lakehouse import get_entities, get_lakehouse

catalog = get_lakehouse()
for name in catalog.list_datasets():
    print(name, get_entities(name).stats())
```

## Custom Dataset Models

Downstream applications can extend the dataset config schema by registering a
[`DatasetModel`](../reference/model.md) subclass process-wide – every config
read (repository construction, `get_dataset_model`, the index export)
constructs through it:

```python
import ftm_lakehouse

class MyModel(ftm_lakehouse.DatasetModel):
    user_id: int = 0

ftm_lakehouse.set_model_class(MyModel)
ftm_lakehouse.update_dataset("my_dataset", user_id=17)
assert ftm_lakehouse.get_dataset_model("my_dataset").user_id == 17
```

Call `set_model_class()` at process start, before any repository or config
access.
