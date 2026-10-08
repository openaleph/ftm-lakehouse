# logic

Pure transformations, with no storage or infrastructure dependencies.

## Entity Aggregation

Fold a stream of statement dicts into entities, without building FtM objects:

```python
from ftm_lakehouse.logic.entities import aggregate_unsafe

for entity in aggregate_unsafe(statement_dicts, "my_dataset"):
    print(f"{entity.id}: {entity.to_dict()['caption']}")
```

The input must be contiguous per `entity_id`, as the parquet store's reads are.

::: ftm_lakehouse.logic.entities.aggregate.aggregate_unsafe
    options:
        heading_level: 3
        show_root_heading: true

## Parquet helpers

The DuckDB SQL behind `ParquetStore`. Reads, merges and re-shards name a partition's files directly (`partition_source_sql`); a read's `statement` view is a plain scan over merged partitions (`live_rows_sql`) and the dedupe query otherwise (`dedupe_rows_sql`). `raw_view_sql` / `live_view_sql` are the `delta_scan` views of the connection-level `LakeStore` behind `stats()` and `statements sql`.

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
