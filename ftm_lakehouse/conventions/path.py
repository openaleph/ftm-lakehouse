"""
The FollowTheMoney data lakehouse specifications fundamental idea is to have a
convention-based file system layout with well-known paths for metadata, and for
information interchange between different processing stages.

All path convention helpers are dataset-specific and relative to their dataset root.
"""

from anystore.util import ensure_uuid, get_extension

from ftm_lakehouse.util import make_checksum_key

INDEX = "index.json"
"""generated index path"""

CONFIG = "config.yml"
"""user editable config path"""

STATISTICS = "statistics.json"
"""computed statistics path"""

VERSIONS = "versions"
"""versions prefix"""


def version(name: str, uuid: str | None = None) -> str:
    """
    Get a version metadata path, e.g. for index.json or stats.json

    Args:
        uuid: identifier, omit to generate one (based on time based uuid7)
    """
    ext = get_extension(name)
    return f"{VERSIONS}/{name}/{ensure_uuid(uuid)}.{ext}"


LOCK = ".LOCK"
"""dataset-wide lock key"""

ARCHIVE = "archive"
"""archive prefix"""


def file_path(checksum: str) -> str:
    """
    Get a file path.

        ./archive/

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


def mapping_prefix(uuid: str | None = None) -> str:
    """
    Get a mapping path prefix

    Args:
        uuid: identifier, omit to generate one
    """
    return f"{MAPPINGS}/{ensure_uuid(uuid)}"


def mapping_yml(uuid: str | None = None) -> str:
    """
    Get a mapping.yml

    Args:
        uuid: identifier, omit to generate one
    """
    return f"{mapping_prefix(uuid)}/{MAPPING}"


def mapping_source(uuid: str, source: str) -> str:
    """
    Get a mapping source file path

    Args:
        uuid: mapping identifier
        source: Source file path
    """
    return f"{mapping_prefix(uuid)}/{source}"


ENTITIES = "entities"
"""entities prefix"""

ENTITIES_JSON = f"{ENTITIES}/entities.ftm.json"
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
