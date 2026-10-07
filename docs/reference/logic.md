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

The DuckDB SQL `ParquetStore` runs: the `statement` views a read picks between – a plain scan (`live_rows_sql`) over merged partitions, the dedupe query (`dedupe_rows_sql`) otherwise – and the merge and re-shard rewrites, which read a partition's files directly (`partition_source_sql`) instead of going through Delta.

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

::: ftm_lakehouse.logic.parquet.live_rows_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.dedupe_rows_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.delta_scan_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.build_merge_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.partition_source_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.read_parquet_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.merge_copy_options
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.worker_duckdb_config
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.build_shard_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.shard_expr_sql
    options:
        heading_level: 3
        show_root_heading: true

## Statement Serialization

Statements are packed once, columnwise, by `ftm_lakehouse.model.statement.statements_to_arrow` – see [Model](model.md#statement-schema).
