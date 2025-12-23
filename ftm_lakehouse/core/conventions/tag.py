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

STORE_OPTIMIZED = "statements/store_optimized"
"""Statement store was optimized and compacted"""

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


def key(key: str) -> str:
    return join_relpaths(TAGS, key)


def mapping_tag(content_hash: str) -> str:
    """Get the tag key for a mapping execution."""
    return f"mappings/{content_hash}/last_processed"
