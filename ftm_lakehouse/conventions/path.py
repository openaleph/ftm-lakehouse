"""
The FollowTheMoney data lakehouse specifications fundamental idea is to have a
convention-based file system layout with well-known paths for metadata, and for
information interchange between different processing stages.

All path convention helpers are dataset-specific and relative to their dataset root.
"""

from datetime import datetime, timezone

from ftm_lakehouse.util import make_checksum_key

INDEX = "index.json"
"""generated index path"""

CONFIG = "config.yml"
"""user editable config path"""

STATISTICS = "statistics.json"
"""computed statistics path"""

VERSIONS = "versions"
"""versions prefix"""


def version(name: str, ts: str | None = None) -> str:
    """
    Get a versioned snapshot path for a file, e.g. for index.json or config.yml

    Layout: versions/YYYY/MM/YYYY-MM-DDTHH:MM:SS/<name>

    Args:
        name: The file name to version (e.g. "config.yml", "index.json")
        ts: ISO timestamp, omit to use current time

    Returns:
        Path like "versions/2025/01/2025-01-15T10:30:00/config.yml"
    """
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()

    year = ts[:4]
    month = ts[5:7]
    return f"{VERSIONS}/{year}/{month}/{ts}/{name}"


LOCK = ".LOCK"
"""dataset-wide lock key"""

LOCK_PREFIX = ".locks/lakehouse"
"""Prefix for specific locks"""

CACHE_PREFIX = ".cache/lakehouse"
"""Prefix for dataset cache"""

ARCHIVE = "archive"
"""archive prefix"""


def file_path(checksum: str) -> str:
    """
    Get a file path for the archive.

    Args:
        checksum: SHA1 checksum of file
    """
    return f"{ARCHIVE}/{make_checksum_key(checksum)}"


def file_path_meta(checksum: str) -> str:
    """
    Get a file metadata path

    Args:
        checksum: SHA1 checksum of fole
    """
    return f"{file_path(checksum)}.json"


def file_path_txt(checksum: str) -> str:
    """
    Get a file text content path

    Args:
        checksum: SHA1 checksum of fole
    """
    return f"{file_path(checksum)}.txt"


MAPPINGS = "mappings"
"""mappings prefix"""

MAPPING = "mapping.yml"
"""mapping file name"""


def mapping_yml(content_hash: str) -> str:
    """
    Get the mapping.yml path for the given file SHA1

    Args:
        uuid: identifier, omit to generate one
    """
    return f"{MAPPINGS}/{content_hash}/{MAPPING}"


ENTITIES = "entities"
"""entities prefix"""

ENTITIES_JSON = "entities.ftm.json"
"""aggregated entities file path"""


STATEMENTS = f"{ENTITIES}/statements"
"""entities statements prefix"""


def origin_prefix(origin: str) -> str:
    """
    Get path prefix for given origin, following parquet partition pattern

    Args:
        origin: The origin, or phase, or stage

    """
    return f"{STATEMENTS}/origin={origin}"


EXPORTS = "exports"
"""exported data prefix"""

EXPORTS_STATISTICS = f"{EXPORTS}/statistics.json"
"""entity counts, pre-computed facts file path"""

EXPORTS_CYPHER = f"{EXPORTS}/graph.cypher"
"""neo4j data export file path"""

EXPORTS_STATEMENTS = f"{EXPORTS}/statements.csv"
"""complete sorted statements file path"""

JOBS = "jobs"
"""Job data prefix"""

JOB_RUNS = f"{JOBS}/runs"
"""Job runs result storage prefix"""
