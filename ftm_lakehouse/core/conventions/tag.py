"""
Global tags used to identify actions. Used for cache keys of workflow runs etc.

Export operations don't have constants here – their freshness tag is the
``path.*`` export target itself (e.g. ``exports/statements.csv``), touched
by `DatasetJobOperation._run_local` after a successful run.
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

OP_EXPORT = "operations/export/last_run"
"""Last fused export sweep (`ExportKind.all`).

The individual artifacts keep their own freshness tags – the sweep stamps
every one it writes – so this is the tag for "the sweep as a whole ran", which
is what a subsequent ``export all`` checks itself against."""

OP_SHARD = "operations/shard/last_run"
"""Last re-shard (statement store rewritten onto a new shard count)"""

OP_MIGRATE = "operations/migrate/last_run"
"""Last migrate (outstanding dataset migrations applied)"""


def migration(name: str) -> str:
    """Applied-marker tag for a single migration.

    Presence, not recency, is the state: a dataset carrying this tag has run
    that migration and
    [`MigrateOperation`][ftm_lakehouse.operation.maintenance.MigrateOperation]
    skips it.

    Args:
        name: Name of a migration function in
            ``ftm_lakehouse.operation.migrations``.
    """
    return f"migrations/{name}"


CRAWL_ORIGIN = "crawl"
"""Default origin identifier for crawled files."""

ARCHIVE_ORIGIN = "archive"
"""Default origin identifier for archived files (if not crawled)"""
