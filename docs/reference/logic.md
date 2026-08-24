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

::: ftm_lakehouse.logic.parquet.build_shard_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.shard_expr_sql
    options:
        heading_level: 3
        show_root_heading: true

The other partition rewrite: `build_shard_sql` re-keys a partition's rows onto a new shard count for `ParquetStore.shard`, recomputing each row's `shard` in DuckDB. `shard_expr_sql` is the SQL twin of `path.entity_shard` – `banal.hash_data` of a string is a plain SHA-1 over its UTF-8 bytes, which is what DuckDB's `sha1()` returns, and a parity test pins the two together.

## Statement Serialization

Statements are packed once, columnwise, by `ftm_lakehouse.model.statement.statements_to_arrow` – see [Model](model.md#statement-schema).
