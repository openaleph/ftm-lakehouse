# logic

The logic module contains pure, stateless transformation functions with no infrastructure dependencies. Functions here take inputs and produce outputs without side effects.

## Entity Aggregation

Aggregate a stream of statement dicts into FollowTheMoney entity dicts:

```python
from ftm_lakehouse.logic.entities import aggregate_unsafe

for entity in aggregate_unsafe(statement_dicts, "my_dataset"):
    print(f"{entity['id']}: {entity['caption']}")
```

`aggregate_unsafe` assumes the input is pre-sorted by `entity_id` – the parquet store guarantees this for its queries.

::: ftm_lakehouse.logic.entities.aggregate.aggregate_unsafe
    options:
        heading_level: 3
        show_root_heading: true

## Parquet helpers

The DuckDB config, the `statement` / `statement_raw` view-SQL builders, and the merge-query builder used by `ParquetStore` via ftmq's `LakeStore`.

::: ftm_lakehouse.logic.parquet.duckdb_config
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.raw_view_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.live_view_sql
    options:
        heading_level: 3
        show_root_heading: true

Both builders emit `delta_scan('<uri>')`, so a view defined from this SQL resolves the current Delta log on every query – defining it once per connection is enough; subsequent `write_deltalake` commits are picked up automatically. The live `statement` view is a plain `WHERE deleted_at IS NULL` scan (no window function, so predicate pushdown survives) and is only correct on an **optimized** store; `statement_raw` exposes every physical row – tombstones and pre-merge duplicates included – for `merge` and raw-source `get_entity_ids` queries (diff exports).

::: ftm_lakehouse.logic.parquet.build_merge_sql
    options:
        heading_level: 3
        show_root_heading: true

An executable DuckDB SQL string over `statement_raw` holding all dedupe / fragment-supersession logic; it collapses one `(shard, bucket, origin)` partition for physical rewrite. Change-detection for diff exports no longer has its own SQL builder – it is an ftmq `Query` over the raw source (`ParquetStore.get_entity_ids(q, source=store.source_raw)`).

## Statement Serialization

Pack and unpack statements for compact storage in the journal `data` column (not to be confused with `ftmq.store.lake.pack_statement`, which packs a statement into a parquet row dict):

```python
from ftm_lakehouse.helpers.statements import pack_journal_row, unpack_journal_row

packed = pack_journal_row(stmt)     # unit-separator delimited string
stmt   = unpack_journal_row(packed) # back to Statement
```

::: ftm_lakehouse.helpers.statements.pack_journal_row
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.helpers.statements.unpack_journal_row
    options:
        heading_level: 3
        show_root_heading: true
