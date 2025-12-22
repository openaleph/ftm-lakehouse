"""Layer 2: Single-purpose storage interfaces.

Each store does one thing and operates on a single storage URI.
No cross-store awareness or business logic.
"""

from ftm_lakehouse.storage.blob import BlobStore
from ftm_lakehouse.storage.file import FileStore
from ftm_lakehouse.storage.journal import JournalStore
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.storage.queue import QueueStore
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.storage.text import TextStore

__all__ = [
    "BlobStore",
    "JournalStore",
    "FileStore",
    "ParquetStore",
    "QueueStore",
    "TagStore",
    "TextStore",
]
