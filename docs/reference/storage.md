# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## SqlJournalStore

SQL statement buffer for write-ahead logging: an append-only, keyless table per dataset carrying the parquet statement columns. A flush rotates the whole journal into a timestamped segment (creating a fresh table in the same DDL transaction), hands the segment over as Arrow batches, and drops it once the consumer has written them – so cleanup never deletes rows, nothing can deadlock against concurrent writers, and a failed write keeps its rows for the next flush. Concurrent flushes on one dataset are serialized by ``flush_lock()``, and only the store that holds the rows drains them – ``ApiJournalStore`` writes, counts and clears, but does not flush. ``get_journal`` resolves the concrete store – ``SqliteJournalStore`` / ``PostgresJournalStore`` locally (picked by uri), ``ApiJournalStore`` when the lakehouse uri points at an API.

::: ftm_lakehouse.storage.journal.sql.SqlJournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet storage for statements, partitioned by ``(shard, bucket, origin)``. Writes are append-only; deduplication, ``first_seen`` folding, and tombstone reaping happen in three independent async ops (``compact`` / ``merge`` / ``vacuum``), all coordinated by a dataset-wide write fence. Reads target a live ``WHERE deleted_at IS NULL`` view with no read-time dedupe, so queries, exports, and statistics assume an optimized store – run ``merge`` before reading. ``shard`` is the odd one out: the only operation that moves rows *between* partitions, rewriting the whole store onto a different shard count.

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
