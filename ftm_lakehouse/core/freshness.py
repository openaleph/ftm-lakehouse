"""Freshness checking utilities."""

from typing import Iterable

from ftm_lakehouse.storage import TagStore


def is_latest(tags: TagStore, key: str, dependencies: Iterable[str]) -> bool:
    """
    Check if a key is more recent than all its dependencies.

    This is a convenience function that wraps TagStore.is_latest().

    Args:
        tags: TagStore instance
        key: Tag key to check
        dependencies: Tag keys that this key depends on

    Returns:
        True if key is newer than all dependencies, False otherwise

    Example:
        ```python
        from ftm_lakehouse.repository.factories import get_tags
        from ftm_lakehouse.core.freshness import is_latest
        from ftm_lakehouse.core.conventions import tag

        tags = get_tags(...)

        if is_latest(tags, tag.ENTITIES_JSON, [tag.STATEMENTS_UPDATED]):
            print("Entities are up-to-date")
        else:
            print("Need to regenerate entities")
        ```
    """
    return tags.is_latest(key, dependencies)
