# logic

The logic module contains pure, stateless transformation functions with no infrastructure dependencies. Functions here take inputs and produce outputs without side effects.

## Entity Aggregation

Aggregate a stream of statement dicts into FollowTheMoney entity dicts:

```python
from ftm_lakehouse.logic import aggregate_unsafe

for entity in aggregate_unsafe(statement_dicts, "my_dataset"):
    print(f"{entity['id']}: {entity['caption']}")
```

`aggregate_unsafe` assumes the input is pre-sorted by `entity_id` – the parquet store guarantees this for its queries.

::: ftm_lakehouse.logic.aggregate_unsafe
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

Both builders emit `delta_scan('<uri>')`, so a view defined from this SQL resolves the current Delta log on every query – defining it once per connection is enough; subsequent `write_deltalake` commits are picked up automatically. The live `statement` view is a plain `WHERE deleted_at IS NULL` scan (no window function, so predicate pushdown survives) and is only correct on an **optimized** store; `statement_raw` exposes every physical row – tombstones and pre-merge duplicates included – for `merge` and `get_changed_entity_ids`.

::: ftm_lakehouse.logic.parquet.build_merge_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.build_changed_sql
    options:
        heading_level: 3
        show_root_heading: true

Both are executable DuckDB SQL strings over `statement_raw`, sharing the dedupe / fragment-supersession logic: `build_merge_sql` collapses one `(shard, bucket, origin)` partition for physical rewrite, `build_changed_sql` returns the canonical live rows of entities changed since a watermark without requiring a merge first.

## Statement Serialization

Pack and unpack statements for compact storage in the journal `data` column:

```python
from ftm_lakehouse.logic import pack_statement, unpack_statement

packed = pack_statement(stmt)     # unit-separator delimited string
stmt   = unpack_statement(packed) # back to Statement
```

::: ftm_lakehouse.helpers.statements.pack_statement
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.helpers.statements.unpack_statement
    options:
        heading_level: 3
        show_root_heading: true
