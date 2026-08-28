"""
Global tags used to identify actions. Used for cache keys of workflow runs etc.

Export operations don't have constants here – their freshness tag is the
``path.*`` export target itself (e.g. ``exports/statements.csv``), touched
by `DatasetJobOperation._run_local` after a successful run.
"""

from ftm_lakehouse.util import validate_origin

STATEMENTS_UPDATED = "statements/last_updated"
"""Statement store was updated"""

JOURNAL_UPDATED = "journal/last_updated"
"""Statement journal was updated"""

JOURNAL_FLUSHED = "journal/last_flushed"
"""Journal store last flushed into statement store"""

STATEMENTS_OPTIMIZED = "statements/last_optimized"
"""Statement store was optimized (merge + compact + vacuum)"""

ARCHIVE_UPDATED = "archive/last_updated"
"""Archive last updated (file added or removed)"""

OP_CRAWL = "operations/crawl/last_run"
"""Last crawl (import files) execution"""

OP_DOWNLOAD_ARCHIVE = "operations/download_archive/last_run"
"""Last download archive execution"""

OP_MAKE = "operations/make/last_run"
"""Last make (full workflow) execution"""

OP_SHARD = "operations/shard/last_run"
"""Last re-shard (statement store rewritten onto a new shard count)"""


def statements_partition_updated(shard: str, bucket: str, origin: str) -> str:
    """Per-partition freshness tag: a ``(shard, bucket, origin)`` was written.

    Partition-scoped analog of [`STATEMENTS_UPDATED`][STATEMENTS_UPDATED],
    stamped by
    [`ParquetStore.append`][ftm_lakehouse.storage.parquet.ParquetStore.append].
    [`ParquetStore.merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge]
    compares it against
    [`statements_partition_optimized`][statements_partition_optimized] via
    [`TagStore.is_latest`][ftm_lakehouse.storage.tags.TagStore.is_latest] to
    skip partitions that haven't changed since their last merge.

    Args:
        shard: Hex-padded shard value.
        bucket: FtM schema bucket (``thing`` / ``interval`` / ...).
        origin: Source tag – validated so it stays a single path segment.
    """
    validate_origin(origin)
    return f"statements/{shard}/{bucket}/{origin}/last_updated"


def statements_partition_optimized(shard: str, bucket: str, origin: str) -> str:
    """Per-partition freshness tag: a ``(shard, bucket, origin)`` was merged.

    Partition-scoped analog of [`STATEMENTS_OPTIMIZED`][STATEMENTS_OPTIMIZED],
    stamped by
    [`ParquetStore.merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge]
    after it rewrites the partition. See
    [`statements_partition_updated`][statements_partition_updated] for the
    freshness comparison.

    Args:
        shard: Hex-padded shard value.
        bucket: FtM schema bucket (``thing`` / ``interval`` / ...).
        origin: Source tag – validated so it stays a single path segment.
    """
    validate_origin(origin)
    return f"statements/{shard}/{bucket}/{origin}/last_optimized"


CRAWL_ORIGIN = "crawl"
"""Default origin identifier for crawled files."""

ARCHIVE_ORIGIN = "archive"
"""Default origin identifier for archived files (if not crawled)"""
