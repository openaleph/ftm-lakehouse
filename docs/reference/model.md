# Layer 1: Model

Pure data structures with no dependencies. Pydantic models for serialization.

## Dataset Models

::: ftm_lakehouse.model.DatasetModel
    options:
        heading_level: 3
        show_root_heading: true

## File Model

::: ftm_lakehouse.model.file.File
    options:
        heading_level: 3
        show_root_heading: true

## Statement Schema

Two schemas, one column apart. `JOURNAL_SCHEMA` is the producer schema – what every write path packs, what the journal table (`journal_table`) physically stores, and what the api wire format carries. `SHARDED_SCHEMA` prepends the `shard` partition key and is what parquet holds; `ParquetStore.append` derives that column from `entity_id`, so no producer carries a shard key of its own and none can route a row against a shard count other than the store's.

`LakehouseStatement` is the statement the write path passes around – ftmq's `LakeStatement` plus `deleted_at`, the tombstone marker. Its `shard` attribute is `EntityBuffer`'s in-memory grouping key, not a packed column. `statements_to_arrow` is the one packer both statement write paths use: ftmq's `statements_to_table` packs the statement columns columnwise, this adds `deleted_at`, drops `canonical_id`, and applies the shared rules (`first_seen` / `last_seen` default, tombstone `last_seen` bump) as vectorized fills.

::: ftm_lakehouse.model.statement.LakehouseStatement
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.statement.statements_to_arrow
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.statement.journal_table
    options:
        heading_level: 3
        show_root_heading: true

## Job Models

::: ftm_lakehouse.model.JobModel
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.DatasetJobModel
    options:
        heading_level: 3
        show_root_heading: true
