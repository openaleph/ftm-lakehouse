# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## JournalStore

SQL statement buffer for write-ahead logging.

::: ftm_lakehouse.storage.JournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet storage for statements, partitioned by ``(shard, bucket, origin)``. Writes are append-only; deduplication, ``first_seen`` folding, and tombstone reaping happen in three independent async ops (``compact`` / ``merge`` / ``vacuum``), all coordinated by a dataset-wide write fence.

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
