"""
Global tags used to identify actions. Used for cache keys of workflow runs etc.
"""

from anystore.util import join_relpaths

TAGS = "tags"
"""Tags cache prefix"""

STATEMENTS_UPDATED = "statements/last_updated"
"""Statement store was updated"""

JOURNAL_UPDATED = "journal/last_updated"
"""Statement journal was updated"""

JOURNAL_FLUSHED = "journal/last_flushed"
"""Journal store last flushed into statement store"""

JOURNAL_FLUSHING = "journal/flushing"
"""Lock key for journal flushing operation"""

STATEMENTS_COMPACTED = "statements/last_compacted"
"""Statement store files were bin-packed (cheap maintenance)"""

STATEMENTS_MERGED = "statements/last_merged"
"""Statement store partitions were merged (dedup, tombstone reap)"""

STATEMENTS_VACUUMED = "statements/last_vacuumed"
"""Obsolete parquet files were removed from disk"""

ARCHIVE_UPDATED = "archive/last_updated"
"""Archive last updated (file added or removed)"""

EXPORTS_STATEMENTS = "exports/statements"
"""Statements CSV export last updated"""

ENTITIES_JSON = "exports/entities_json"
"""Entities JSON export last updated"""

STATISTICS = "exports/statistics"
"""Statistics export last updated"""

OP_CRAWL = "operations/crawl/last_run"
"""Last crawl (import files) execution"""

OP_DOWNLOAD_ARCHIVE = "operations/download_archive/last_run"
"""Last download archive execution"""

OP_MAKE = "operations/make/last_run"
"""Last make (full workflow) execution"""


def key(key: str) -> str:
    return join_relpaths(TAGS, key)


def mapping_tag(content_hash: str) -> str:
    """Get the tag key for a mapping execution."""
    return f"mappings/{content_hash}/last_processed"


DEFAULT_ORIGIN = "default"
"""Default origin identifier"""

CRAWL_ORIGIN = "crawl"
"""Default origin identifier for crawled files."""

ARCHIVE_ORIGIN = "archive"
"""Default origin identifier for archived files (if not crawled)"""
