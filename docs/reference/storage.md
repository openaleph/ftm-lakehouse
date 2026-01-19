# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## BlobStore

Content-addressed blob storage.

::: ftm_lakehouse.storage.BlobStore
    options:
        heading_level: 3
        show_root_heading: true

## FileStore

JSON metadata file storage.

::: ftm_lakehouse.storage.FileStore
    options:
        heading_level: 3
        show_root_heading: true

## JournalStore

SQL statement buffer for write-ahead logging.

::: ftm_lakehouse.storage.JournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet storage for statements.

::: ftm_lakehouse.storage.ParquetStore
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
