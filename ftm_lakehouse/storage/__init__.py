"""Layer 2: Single-purpose storage interfaces.

Each store does one thing and operates on a single storage URI.
No cross-store awareness or business logic.
"""

from ftm_lakehouse.storage.journal import JournalStore
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.storage.queue import QueueStore
from ftm_lakehouse.storage.tags import TagStore

__all__ = [
    "JournalStore",
    "ParquetStore",
    "QueueStore",
    "TagStore",
]
