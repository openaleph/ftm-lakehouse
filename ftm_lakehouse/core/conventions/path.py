"""
Path conventions for the FollowTheMoney data lakehouse.

The fundamental idea is to have a convention-based file system layout with
well-known paths for metadata and information interchange between processing stages.

All paths are dataset-relative unless otherwise noted.

Dataset Layout
--------------

::

    lakehouse/
        index.json                          # catalog index
        config.yml                          # catalog configuration
        versions/                           # versioned snapshots
            YYYY/MM/YYYY-MM-DDTHH:MM:SS/
                index.json
                config.yml

        [dataset]/
            index.json                      # dataset index
            config.yml                      # dataset configuration

            versions/                       # versioned snapshots
                YYYY/MM/...

            .LOCK                           # dataset-wide maintenance lock
            .LOCK-APPENDS/                  # in-flight append markers
            .locks/{tenant}/                 # operation-specific locks
            tags/{tenant}/                  # workflow state / cache

            archive/                        # content-addressed file storage
                ab/cd/ef/{checksum}/        # SHA256 split into segments
                    blob                    # file blob (stored once)
                    {file_id}.json          # metadata (one per source path)
                    {origin}.txt            # extracted text (one per engine)

            statements/                     # statement store (shard-partitioned)
                shard={shard}/
                    bucket={bucket}/
                        origin={origin}/
                            *.parquet

            entities.ftm.json[.zst|gzip]    # aggregated entities export

            exports/
                statistics.json             # entity counts, facets
                statements.csv[.zst|gzip]   # sorted statements
                documents.csv[.zst|gzip]    # document metadata
                documents.{origin}.csv[...] # document metadata (origin-scoped)
                graph.cypher                # neo4j export (optional)

            diffs/                          # dirs are codec-free (they double
                entities.ftm.json/          #   as freshness tags); the files
                    {ts}.delta.json[.zst|gzip]         # entities delta
                exports/
                    documents.csv/
                        {ts}.diff.csv[.zst|gzip]       # documents delta
                    documents.{origin}.csv/
                        {ts}.diff.csv[.zst|gzip]       # origin-scoped delta

            jobs/
                runs/
                    {job_type}/
                        {timestamp}.json    # job run results
"""

from datetime import datetime

from ftm_lakehouse.logic.path import (
    CallableKey,
    DateTimeKey,
    JobsKey,
    ScopedKey,
    StoreKey,
    make_ts,
)
from ftm_lakehouse.util import make_checksum_key, safe_name, validate_origin

TENANT = "lakehouse"
"""Default tenant name"""

INDEX = StoreKey("index.json")
"""generated index filename"""

CONFIG = StoreKey("config.yml")
"""user editable config filename"""

STATISTICS = StoreKey("statistics.json")
"""computed statistics filename"""

TS_FORMAT = "%Y%m%dT%H%M%S%fZ"
"""Global format for timestamps in files"""


class VersionsKey(CallableKey):
    """``versions/``: a prefix to iterate, and a factory for one snapshot."""

    def __init__(self) -> None:
        super().__init__("versions")

    def __call__(self, name: str, ts: datetime | str | None = None) -> StoreKey:
        """Get a versioned snapshot path for a file (``index.json``, ``config.yml``).

        Layout: ``versions/YYYY/MM/{TS_FORMAT}/<name>``

        Args:
            name: The file name to version (e.g. ``config.yml``, ``index.json``)
            ts: Timestamp of the snapshot, omit to use current time

        Returns:
            Key like ``versions/2025/01/20250115T103000000000Z/config.yml``
        """
        if not isinstance(ts, str):
            ts = make_ts(ts, TS_FORMAT)
        return self / ts[:4] / ts[4:6] / ts / name


VERSIONS = VersionsKey()
"""Base path for versions, and the factory for one snapshot"""

LOCK = StoreKey(".LOCK")
"""dataset-wide maintenance lock key name"""

LOCK_APPENDS = StoreKey(".LOCK-APPENDS")
"""Prefix for per-writer append marker keys (shared side of the write fence)"""

LOCKS = ScopedKey(".locks", TENANT)
"""Locks, under the default tenant: ``.locks/lakehouse/``.
``LOCKS["other"]`` for another tenant."""

TAGS = ScopedKey("tags", TENANT)
"""Freshness tags, under the default tenant: ``tags/lakehouse/``
``TAGS["other"]`` for another tenant."""

ARCHIVE = "archive"
"""Base path for archive"""

ARCHIVE_BLOB = "blob"
"""blob filename within checksum directory"""


class ArchiveKey(StoreKey):
    """The directory holding one archived file, and the files in it.

    Layout: ``archive/5a/6a/cf/5a6acf229ba576d9a40b09292595658bbb74ef56/``

    One checksum is stored once, but it can have arrived by several source
    paths and been read by several text extractors – hence
    [`meta`][ArchiveKey.meta] and [`txt`][ArchiveKey.txt] being keyed rather
    than fixed like [`blob`][ArchiveKey.blob].

    Args:
        checksum: SHA256 checksum of the file
    """

    def __init__(self, checksum: str) -> None:
        super().__init__(ARCHIVE, make_checksum_key(checksum))

    @property
    def blob(self) -> StoreKey:
        """The file's content, stored once per checksum."""
        return self / ARCHIVE_BLOB

    def meta(self, file_id: str) -> StoreKey:
        """Metadata for one file instance.

        Several files with the same checksum but different source paths each
        get their own metadata, keyed by their ``File.id``.

        Layout: ``archive/5a/6a/cf/.../file-abc123.json``

        Args:
            file_id: The ``File.id`` (hash of source path + checksum)

        Raises:
            ValueError: If ``file_id`` is malformed.
        """
        return self / f"{safe_name(file_id, 'file_id')}.json"

    def txt(self, origin: str) -> StoreKey:
        """Extracted text for one extraction origin.

        Several extractions can exist per file, keyed by origin (different OCR
        engines or extraction methods).

        Layout: ``archive/5a/6a/cf/.../{origin}.txt``

        Args:
            origin: The extraction origin / engine name

        Raises:
            ValueError: If ``origin`` is malformed.
        """
        return self / f"{validate_origin(origin)}.txt"


ENTITIES_JSON = StoreKey("entities.ftm.json")
"""aggregated entities export – the identity; ``+ compression`` for the artifact"""


STATEMENTS = "statements"
"""Base path for storing statement data (partitioned by shard, bucket, origin)"""


EXPORTS = StoreKey("exports")
"""Base path for exports"""

EXPORTS_STATISTICS = EXPORTS / STATISTICS
"""entity counts, pre-computed facts file path"""

EXPORTS_CYPHER = EXPORTS / "graph.cypher"
"""neo4j data export file path"""

EXPORTS_STATEMENTS = EXPORTS / "statements.csv"
"""complete sorted statements export – codec-free"""

EXPORTS_DOCUMENTS = EXPORTS / "documents.csv"
"""documents metadata export – codec-free, unscoped"""


DIFFS = StoreKey("diffs")
"""Base path for diff exports"""

EXT_ENTITIES_DELTA = "delta.json"
"""Extension of one entities diff file"""

EXT_DOCUMENTS_DELTA = "diff.csv"
"""Extension of one documents diff file"""


DIFFS_ENTITIES = DateTimeKey(DIFFS / ENTITIES_JSON, TS_FORMAT, EXT_ENTITIES_DELTA)
"""Entities diff series: ``DIFFS_ENTITIES(ts) + compression``"""

DIFFS_DOCUMENTS = DateTimeKey(DIFFS / EXPORTS_DOCUMENTS, TS_FORMAT, EXT_DOCUMENTS_DELTA)
"""Documents diff series: ``DIFFS_DOCUMENTS[origin](ts) + compression``"""


JOBS = StoreKey("jobs")
"""Job data prefix"""

JOB_RUNS = JobsKey(JOBS / "runs")
"""Job runs result storage prefix, and the factory for one run"""
