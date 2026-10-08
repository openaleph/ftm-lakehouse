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

Two schemas, one column apart. `JOURNAL_SCHEMA` is what every write path packs, the journal table (`journal_table`) stores and the api wire carries. `SHARDED_SCHEMA` adds the `shard` partition key and is what parquet holds – `ParquetStore.append` derives it from `entity_id`.

`LakehouseStatement` is ftmq's `LakeStatement` plus the two columns the lakehouse adds: `deleted_at` (the tombstone marker) and `role` (who asserted the statement). `statements_to_arrow` packs statements into a `JOURNAL_SCHEMA` table for both write paths and applies their shared fill rules.

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
