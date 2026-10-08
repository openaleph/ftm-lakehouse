# Working with Entities

The entities repository is the main way to read, write and query [FollowTheMoney](https://followthemoney.tech) data in `ftm-lakehouse`.

## Overview

Entities are stored as **statements** – one record per property value. That gives:

- **Versioning**: `first_seen` / `last_seen` per statement
- **Provenance**: `origin`, `role`, `original_value` and the rest of the [Statement model](https://followthemoney.tech/docs/statements/)
- **Incremental updates**: new data is appended, nothing is reprocessed
- **Simple identity**: entities are keyed on `entity_id`. A dataset has no cross-source resolution, so `canonical_id` is not stored (it always equals `entity_id`)

Each dataset is one Delta Lake table, partitioned by `(shard, bucket, origin)` – see [Sharded append-only pattern](../architecture.md#sharded-append-only-pattern). Writes are append-only. Reads reconcile duplicates, superseded fragments and tombstones, and the `optimize` operation compacts them on disk.

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

`get_entities` returns one cached `EntityRepository` per dataset, shared by the library, CLI, operations and API server.

## Writing Entities

Writes go to the journal and become readable once flushed into parquet.

### Single Entity

```python
from followthemoney import model
from ftm_lakehouse import ensure_dataset, get_entities

ensure_dataset("my_dataset")
entities = get_entities("my_dataset")

entity = model.make_entity("Person")
entity.id = "jane-doe"
entity.add("name", "Jane Doe")
entity.add("nationality", "us")

entities.add(entity, origin="manual")
```

### Bulk Writing (through the journal)

```python
with entities.writer(origin="bulk_import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

The writer inserts into the journal in batches – an append-only SQL table with the parquet store's columns. If the block raises, it drops what it has not inserted yet; batches already inserted stay. `flush()` drains the journal into parquet:

```python
count = entities.flush()
print(f"Flushed {count} statements")
```

### Bulk Import (bypassing the journal)

For one-shot loads, such as millions of entities from an exported file, skip the journal and write to parquet through an in-memory buffer:

```python
from datetime import datetime, timezone
from ftmq.io import smart_read_proxies
from ftm_lakehouse.logic.entities.buffer import EntityBuffer

repo = get_entities("my_dataset")
buffer = EntityBuffer(repo.dataset, origin="bulk")
now = datetime.now(timezone.utc)

for proxy in smart_read_proxies("entities.ftm.json"):
    buffer.add_entity(proxy)
    if len(buffer) >= 1_000_000:
        repo.write_batches([buffer.flush_table(now)])

if buffer:
    repo.write_batches([buffer.flush_table(now)])
```

`EntityBuffer` collapses repeated statements per `(id, origin, fragment, role)`. `flush_table()` empties it as one Arrow table, and `write_batches` appends that as one parquet file per `(shard, bucket, origin)` partition it spans. Adding past `LAKEHOUSE_MAX_BUFFER_ROWS` raises `BufferFullError`, so flush before that.

`ftm-lakehouse entities import` runs this loop.

## Reading Entities

!!! note "Reads reconcile – `optimize` is an optimisation"
    Reads collapse duplicates, apply fragment supersession and hide tombstones (see [Deduplication](#deduplication)), so `query`, exports and statistics are correct before the next `optimize`. Run `optimize` on a schedule for read speed and disk space.

Reads see the parquet store, not the journal – pass `flush_first=True` to `get` / `query` to drain the journal first.

### Get by ID

```python
entity = entities.get("jane-doe")
if entity:
    print(entity.caption)
```

### Query with Filters

Filters are an [ftmq `Query`](https://docs.investigraph.dev/lib/ftmq/query) built from filter nodes: `M` for meta fields (`entity_id`, `schema`, …), `P` for entity properties and `C` for storage columns such as `origin` and `role`:

```python
from ftmq.query import C, M, Query

for entity in entities.query(Query(C(origin="import"))):
    print(entity.id)

ids = ["jane-doe", "john-smith"]
for entity in entities.query(Query(M(entity_id__in=ids))):
    print(entity.caption)

# schema and entity_id filters also skip the partitions that cannot match
for entity in entities.query(Query(M(schema="Person"))):
    print(entity.schema.name)
```

`query_statements` takes the same `Query` and yields the statements (`LakehouseStatement`) instead of entities.

### Stream from Exported File

```python
for entity in entities.stream():
    process(entity)
```

`stream()` reads the last export, `entities.ftm.json`. For a full pass that is faster than aggregating the statement store, but only as fresh as the export. On the CLI, `entities stream` reads the export and `entities iterate` the live store.

## The Origin Field

`origin` records where data came from. It is a partition key, so filters on it are cheap:

```python
with entities.writer(origin="source_a") as writer:
    for entity in source_a_entities:
        writer.add_entity(entity)

with entities.writer(origin="source_b") as writer:
    for entity in source_b_entities:
        writer.add_entity(entity)

for entity in entities.query(Query(C(origin="source_a"))):
    print(entity.id)
```

`entities.delete_origin("source_a")` drops an origin physically – stop its writers first.

## The Role Field

`role` records *who* asserted a statement – an id the submitting application supplies for the user, service account or other actor behind a write:

```python
with entities.writer(origin="webui", role="user:42") as writer:
    writer.add_entity(entity)

# a per-statement override, for producers that mix roles in one batch
with entities.writer(origin="webui", role="user:42") as writer:
    writer.add_statement(stmt, role="user:7")
```

`role` is optional; `None` is stored as NULL. It is not part of the statement `id`, so identical content gets the same id whoever asserts it.

### Roles are row identity, not a last-writer-wins field

Like `origin` and `fragment`, `role` is part of a row's identity, so **two roles asserting identical content keep two rows**:

```python
with entities.writer(role="user:42") as writer:
    writer.add_statement(stmt)
with entities.writer(role="user:7") as writer:
    writer.add_statement(stmt)  # same content

entities.merge()
# two rows, one per role
```

One role re-asserting the same content still collapses to one row, which keeps its original `first_seen`. A role's first assertion of content another role already wrote is a new row with its own `first_seen`, so diff exports report it.

The assembled entity is unchanged – a property holds each distinct value once:

```python
entity = entities.get("acme")
assert entity.get("name") == ["Acme Inc"]  # not duplicated per role
```

Filter with the `C` family:

```python
from ftmq.query import C, Query

for entity in entities.query(Query(C(role="user:42"))):
    print(entity.id)
```

Like `C(origin=...)`, this selects the *entities* with a matching statement, not single rows.

### Deletes are per row

`delete_entity` tombstones every live row of the entity, whatever its role. To remove one role's assertion only, read the row back and tombstone it:

```python
target = next(
    s for s in entities.query_statements(Query(M(entity_id="acme")))
    if s.role == "user:42"
)
entities.delete_statement(target)  # the read-back statement carries its role
```

For a hand-built `Statement`, pass `role=` (and `fragment=`) to `delete_statement` – a tombstone with the wrong role shadows nothing.

### Round-tripping

`statements.csv` has a `role` column, so a statement export and re-import keeps every role. `entities.ftm.json` holds one payload per entity with `role` as a list: on re-import a single role is recovered, while several are ambiguous and fall back to the import default, as with `origin`. Use the statement export when per-row roles must survive.

On the CLI, `--role` is the default for input that carries none; a payload's own `role` wins. There is no `--override-role`.

```bash
ftm-lakehouse -d my_dataset entities import --role user:42 -i entities.ftm.json
```

## Fragment Supersession

A statement is written in one of two modes. By default (**non-fragment**) dedup is content-addressed: each statement `id` lives on its own `last_seen`, and distinct ids never interact.

With a `fragment`, a statement is in **supersession** mode, like the `fragment` column of [followthemoney-store](https://github.com/alephdata/followthemoney-store): a later emission for the same `(entity_id, prop, fragment)` replaces the older one, even though changed values have different statement ids.

```python
with entities.writer(origin="csv_import") as writer:
    writer.add_entity(company, fragment="row42")

# later, the source row changed – re-emit under the same fragment:
with entities.writer(origin="csv_import") as writer:
    writer.add_entity(updated_company, fragment="row42")

# after flush, only the updated values are visible
```

Typical use is one fragment per source row in a CSV ingest, or per document in a crawler, so re-processing a source replaces what that row said instead of accumulating stale values. `add_statement` takes the same parameter.

### Semantics

- **Scope is per `(entity_id, prop, fragment)`**, not the whole fragment. If the first emission had `name`, `address` and `country` and the re-emission only `name` and `address`, the old `country` stays. For whole-fragment replacement, tombstone the dropped props: a statement read back from the store carries its fragment, so `delete_statement(stmt)` hits the right group; pass `fragment=` only for hand-built statements.
- **Multi-valued props survive together.** All rows of one emission share a `last_seen`, so all values of the latest emission are kept and all values of older ones go.
- **The two modes are isolated.** A non-fragment row never supersedes a fragment row or vice versa, even with identical content. `fragment` is part of the row identity but not of the statement `id`, so the same statement can exist under several fragments and without one.
- **Origins and roles are isolated too.** The same fragment under two origins, or written by two roles, forms independent groups.
- **Tombstones participate.** A tombstone with the fragment supersedes its group like any emission: the group disappears from reads at once and is removed from disk once the tombstone passes the grace period. `delete_entity` writes fragment-matched tombstones itself.

### Producer contract

All rows of one fragment emission **must share one `last_seen`** – supersession keeps the rows tied at the group's latest `last_seen`, so jitter within an emission would keep only the latest row. `add_entity` pins one timestamp per fragment emission (the entity's `last_seen` / `last_change`, else one `now`). Non-fragment statements keep their own `last_seen` and fall back to the pinned value only when unset. Statement-level producers set one timestamp per emission themselves:

```python
from datetime import datetime, timezone
from followthemoney import Statement

ts = datetime.now(timezone.utc).isoformat()
with entities.writer(origin="import") as writer:
    for prop, value in row_values:
        writer.add_statement(
            Statement(entity_id=entity_id, prop=prop, value=value, schema=schema, dataset="my_dataset", last_seen=ts),
            fragment=f"row{row_number}",
        )
```

Distinct emissions need distinct timestamps: two emissions of one fragment with the same `last_seen` tie, and both survive.

"No fragment" is stored as the empty string, never NULL; `fragment=None` becomes `''`.

## Deleting Entities

Deletes are tombstones – rows with `deleted_at` set, written through the journal (or `EntityBuffer` on the bulk path). A deleted entity disappears from `query()` once the tombstone is flushed; `stream()` shows it until the next export. `merge` removes the tombstone and the rows it shadows once the tombstone is older than the grace period.

### Delete an Entity

```python
count = entities.delete_entity("jane-doe")
print(f"Wrote {count} tombstones")
entities.flush()
```

`origin=` limits the delete to one origin's statements.

### Delete a Single Statement

```python
target = next(entities.query_statements())
entities.delete_statement(target)
entities.flush()
```

### Re-adding After Delete

```python
entities.delete_entity("jane-doe")
entities.flush()

entities.add(updated_jane, origin="correction")
entities.flush()
# jane-doe is alive again with the new data
```

A re-added statement is newer than its tombstone, so it wins with or without a merge in between.

## Deduplication

Nothing collapses on write, except inside one writer batch, where the buffer keys rows by `(id, origin, fragment, role)`. The journal is append-only, so re-flushing a statement appends another parquet row. Reads collapse them, and `merge` does so on disk: it keeps the row with the latest `last_seen` per statement `id` and role (per supersession group for fragment rows) and folds `first_seen` to the earliest.

```python
entities.add(entity)
entities.flush()   # one row in parquet
entities.add(entity)
entities.flush()   # two rows, one statement id – reads return one

entities.merge()   # one row: last_seen=now, first_seen=original
```

## Maintenance

Operations on the statement store, serialised by the dataset's [locks](../architecture.md#sharded-append-only-pattern): ingest keeps flowing through a merge, while the in-place rewrites (re-shard, `delete_origin`, `vacuum`) make appends wait. On a schedule, run merge and vacuum together as `optimize` – `ftm-lakehouse -d my_dataset maintenance optimize` or `ftm_lakehouse.operation.optimize("my_dataset")`.

### Flush (journal → parquet)

```python
count = entities.flush()
```

Rotates the journal away and streams it into parquet; writers continue into a fresh journal table meanwhile. Nothing is deduplicated here. A concurrent flush makes this one a no-op, so `0` does not mean the journal is empty.

From the CLI, per dataset or across the whole catalog:

```bash
ftm-lakehouse -d my_dataset maintenance flush
ftm-lakehouse maintenance flush --all
```

### Merge (expensive)

Flushes, then rewrites each dirty partition: collapses duplicates and superseded fragment emissions, folds `first_seen` to the earliest and drops tombstones older than the grace period, with the rows they shadow. `force=True` rewrites clean partitions too.

```python
entities.merge()
```

The grace period is `LAKEHOUSE_GRACE_PERIOD_DAYS` (default 30); `0` drops tombstones at once.

### Vacuum

Deletes the parquet files that `merge` replaced:

```python
entities.statements.vacuum()
entities.statements.vacuum(retention_hours=24)  # keep files replaced in the last day
```

## Complete Example

```python
from followthemoney import EntityProxy, model
from ftm_lakehouse import ensure_dataset, get_entities


def create_person(name: str, nationality: str) -> EntityProxy:
    entity = model.make_entity("Person")
    entity.make_id(name)
    entity.add("name", name)
    entity.add("nationality", nationality)
    return entity


def main():
    ensure_dataset("people_dataset")
    entities = get_entities("people_dataset")

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
