# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## JournalStore

SQL statement buffer for write-ahead logging.

::: ftm_lakehouse.storage.JournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet storage for statements. Uses a sidecar metadata table for tracking timestamps and soft deletes.

::: ftm_lakehouse.storage.ParquetStore
    options:
        heading_level: 3
        show_root_heading: true

## SidecarStore

Lightweight Delta table for per-statement metadata (`first_seen`, `last_seen`, `deleted_at`). Used internally by ParquetStore.

::: ftm_lakehouse.storage.parquet.SidecarStore
    options:
        heading_level: 3
        show_root_heading: true

## TagStore

Key-value freshness tracking.

::: ftm_lakehouse.storage.TagStore
    options:
        heading_level: 3
        show_root_heading: true

## QueueStore

CRUD action queue for async processing.

::: ftm_lakehouse.storage.QueueStore
    options:
        heading_level: 3
        show_root_heading: true
