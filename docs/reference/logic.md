# logic

Pure transformations, with no storage or infrastructure dependencies.

## Entity Aggregation

Fold a stream of statement dicts into entities, without building FtM objects – [`ftmq.aggregate`](https://docs.investigraph.dev/lib/ftmq/reference/aggregate/):

```python
from ftmq.aggregate import aggregate_statement_payloads

for entity in aggregate_statement_payloads(statement_dicts, "my_dataset"):
    print(f"{entity.id}: {entity.to_dict()['caption']}")
```

The input must be contiguous per `entity_id`, as the parquet store's reads are.

## Parquet helpers

The DuckDB SQL behind `ParquetStore`. Reads, merges and re-shards name a partition's files directly (`partition_source_sql`); a read's `statement` view is a plain scan over merged partitions (`live_rows_sql`) and the dedupe query otherwise (`dedupe_rows_sql`). `raw_view_sql` is the connection-level `LakeStore`'s `statement_raw` view over `delta_scan`; `statements sql` runs on `ParquetStore.cursor`, the whole table registered like a read.

::: ftm_lakehouse.logic.parquet.duckdb_config
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.raw_view_sql
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

::: ftm_lakehouse.logic.parquet.relation_source_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.read_parquet_sql
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

## Merge helpers

The pure Arrow side of `ParquetStore.merge`: a partition is read as `entity_id`-sorted streams and cut into entity-closed chunks, each deduped by `build_merge_sql` on its own – every window key holds `entity_id`, so the chunks give the rows the whole partition would, with memory bounded by the chunk rather than the partition.

::: ftm_lakehouse.logic.merge.entity_chunks
    options:
        heading_level: 3
        show_root_heading: true

## Statement Serialization

Statements are packed once, columnwise, by `ftm_lakehouse.model.statement.statements_to_arrow` – see [Model](model.md#statement-schema).
