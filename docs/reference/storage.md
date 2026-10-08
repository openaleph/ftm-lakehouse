# Layer 2: Storage

Single-purpose storage interfaces. Each store does one thing.

## SqlJournalStore

Write-ahead log for statements: one append-only, keyless table per dataset in `JOURNAL_SCHEMA`. A flush renames the table to a timestamped segment, recreating it in the same DDL transaction, hands the segment over as Arrow tables and drops it once the consumer has written them – a failed write keeps its rows for the next flush. On postgres the rename waits at most `lock_timeout` (5s) for in-flight writers; a blocked flush fails and the next one retries. `flush_lock()` serializes flushes per dataset. `get_journal` picks the store: `SqliteJournalStore` / `PostgresJournalStore` by uri, or `ApiJournalStore` in api mode, which only writes – the server drains its journal.

::: ftm_lakehouse.storage.journal.sql.SqlJournalStore
    options:
        heading_level: 3
        show_root_heading: true

## ParquetStore

Delta Lake parquet store for statements, partitioned by `(shard, bucket, origin)`. Writes are append-only; reads reconcile duplicates, superseded fragments and tombstones, except over partitions made of `merge` output alone, which read as a plain scan. `merge` (under the merge lock, so appends keep flowing) and `vacuum` (under the exclusive `.LOCK` too) are maintenance; `shard` is the one operation that moves rows between partitions, onto a new shard count.

::: ftm_lakehouse.storage.parquet.ParquetStore
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.storage.parquet.merge_partition
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
