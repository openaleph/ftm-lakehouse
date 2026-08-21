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

`SHARDED_SCHEMA` is the single source of truth for the physical statement columns: the parquet store, the journal table (`journal_table`) and the api wire format all speak it. `LakehouseStatement` is the statement the write path passes around – ftmq's `LakeStatement` plus the two columns the lakehouse adds, `shard` and `deleted_at`. `statements_to_arrow` is the one packer both statement write paths use: ftmq's `statements_to_table` packs the statement columns columnwise, this adds `shard` and `deleted_at`, drops `canonical_id`, and applies the shared rules (`first_seen` / `last_seen` default, tombstone `last_seen` bump) as vectorized fills.

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
