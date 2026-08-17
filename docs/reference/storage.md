# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## SqlJournalStore

SQL statement buffer for write-ahead logging. ``get_journal`` resolves the concrete store – ``SqlJournalStore`` locally, ``ApiJournalStore`` when the lakehouse uri points at an API.

::: ftm_lakehouse.storage.journal.sql.SqlJournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet storage for statements, partitioned by ``(shard, bucket, origin)``. Writes are append-only; deduplication, ``first_seen`` folding, and tombstone reaping happen in three independent async ops (``compact`` / ``merge`` / ``vacuum``), all coordinated by a dataset-wide write fence. Reads target a live ``WHERE deleted_at IS NULL`` view with no read-time dedupe, so queries, exports, and statistics assume an optimized store – run ``merge`` before reading.

::: ftm_lakehouse.storage.parquet.ParquetStore
    options:
        heading_level: 3
        show_root_heading: true

## TagStore

Key-value freshness tracking.

::: ftm_lakehouse.storage.tags.TagStore
    options:
        heading_level: 3
        show_root_heading: true

## VersionStore

Timestamped snapshots for config / index files.

::: ftm_lakehouse.storage.versions.VersionStore
    options:
        heading_level: 3
        show_root_heading: true
