"""
Global tags used to identify actions. Used for cache keys of workflow runs etc.
"""

TAGS = "tags"
"""Tags cache prefix"""

STATEMENTS_UPDATED = "statements/last_updated"
"""Statement store was updated"""

FRAGMENTS_UPDATED = "fragments/last_updated"
"""Fragment store was updated"""

FRAGMENTS_COLLECTED = "fragments/last_collected"
"""Fragment store last collected into statements"""

STORE_OPTIMIZED = "statements/store_optimized"
"""Statement store was optimized and compacted"""


def key(key: str) -> str:
    return f"{TAGS}/{key}"
