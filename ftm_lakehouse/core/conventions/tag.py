"""
Global tags used to identify actions. Used for cache keys of workflow runs etc.

Export operations don't have constants here – their freshness tag is the
``path.*`` export target itself (e.g. ``exports/statements.csv``), touched
by :meth:`DatasetJobOperation._run_local` after a successful run.
"""

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


def mapping_tag(content_hash: str) -> str:
    """Get the tag key for a mapping execution."""
    return f"mappings/{content_hash}/last_processed"


DEFAULT_ORIGIN = "default"
"""Default origin identifier"""

CRAWL_ORIGIN = "crawl"
"""Default origin identifier for crawled files."""

ARCHIVE_ORIGIN = "archive"
"""Default origin identifier for archived files (if not crawled)"""
