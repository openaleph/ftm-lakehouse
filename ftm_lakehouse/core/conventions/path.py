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

            .LOCK                           # dataset-wide lock
            locks/{tenant}/                 # operation-specific locks
            tags/{tenant}/                  # workflow state / cache
            queue/{tenant}/                 # task queues

            archive/                        # content-addressed file storage
                ab/cd/ef/{checksum}/        # SHA1 split into segments
                    data                    # file blob (stored once)
                    {file_id}.json          # metadata (one per source path)
                    {origin}.txt            # extracted text (one per engine)

            mappings/
                {content_hash}/
                    mapping.yml             # current CSV mapping configuration
                    versions/               # versioned snapshots
                        YYYY/MM/...

            entities/
                statements/                 # statement store (partitioned)
                    origin={origin}/
                        *.parquet

            entities.ftm.json               # aggregated entities export

            exports/
                statistics.json             # entity counts, facets
                statements.csv              # sorted statements
                graph.cypher                # neo4j export (optional)

            jobs/
                runs/
                    {job_type}/
                        {timestamp}.json    # job run results
"""

from datetime import datetime, timezone

from anystore.util import ensure_uuid, join_relpaths

from ftm_lakehouse.util import make_checksum_key

TENANT = "lakehouse"
"""Default tenant name"""

INDEX = "index.json"
"""generated index filename"""

CONFIG = "config.yml"
"""user editable config filename"""

STATISTICS = "statistics.json"
"""computed statistics filename"""

VERSIONS = "versions"
"""Base path for versions"""


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
"""dataset-wide lock key name"""

LOCKS = "locks"
"""Base path for storing locks"""


def lock(*parts: str, tenant: str | None = TENANT) -> str:
    """Generate a path to store a lock"""
    return join_relpaths(LOCKS, tenant or TENANT, *parts)


TAGS = "tags"
"""Base path for dataset tags cache"""


def tag(*parts: str, tenant: str | None = TENANT) -> str:
    """Generate a path to store a tag"""
    return join_relpaths(TAGS, tenant or TENANT, *parts)


ARCHIVE = "archive"
"""Base path for archive"""

ARCHIVE_BLOB = "data"
"""blob filename within checksum directory"""


def archive_prefix(checksum: str) -> str:
    """
    Get the directory path for a file in the archive.

    Layout: archive/5a/6a/cf/5a6acf229ba576d9a40b09292595658bbb74ef56/

    Args:
        checksum: SHA1 checksum of file
    """
    return f"{ARCHIVE}/{make_checksum_key(checksum)}"


def archive_blob(checksum: str) -> str:
    """
    Get the blob path for a file in the archive.

    Layout: archive/5a/6a/cf/5a6acf229ba576d9a40b09292595658bbb74ef56/data

    Args:
        checksum: SHA1 checksum of file
    """
    return f"{archive_prefix(checksum)}/{ARCHIVE_BLOB}"


def archive_meta(checksum: str, file_id: str) -> str:
    """
    Get a file metadata path for a specific file instance.

    Multiple files with the same checksum but different source paths
    each get their own metadata file, keyed by their File.id.

    Layout: archive/5a/6a/cf/.../file-abc123.json

    Args:
        checksum: SHA1 checksum of file
        file_id: The File.id (hash of source path + checksum)
    """
    return f"{archive_prefix(checksum)}/{file_id}.json"


def archive_txt(checksum: str, origin: str) -> str:
    """
    Get a file text content path for a specific extraction origin.

    Multiple text extractions can exist per file, keyed by origin
    (e.g., different OCR engines or extraction methods).

    Layout: archive/5a/6a/cf/.../{origin}.txt

    Args:
        checksum: SHA1 checksum of file
        origin: The extraction origin/engine name
    """
    return f"{archive_prefix(checksum)}/{origin}.txt"


MAPPINGS = "mappings"
"""Base path for storing mappings"""

MAPPING = "mapping.yml"
"""mapping file name"""


def mapping(content_hash: str) -> str:
    """
    Get the mapping.yml path for the given file SHA1.

    Layout: mappings/{content_hash}/mapping.yml
    """
    return f"{MAPPINGS}/{content_hash}/{MAPPING}"


ENTITIES = "entities"
"""Base path for storing entities data"""

ENTITIES_JSON = "entities.ftm.json"
"""aggregated entities file name"""


STATEMENTS = f"{ENTITIES}/statements"
"""Base path for storing statement data"""


def statement_origin(origin: str) -> str:
    """
    Get path prefix for given origin, following parquet partition pattern

    Args:
        origin: The origin, or phase, or stage
    """
    return f"{STATEMENTS}/origin={origin}"


EXPORTS = "exports"
"""Base path for exports"""

EXPORTS_STATISTICS = f"{EXPORTS}/{STATISTICS}"
"""entity counts, pre-computed facts file path"""

EXPORTS_CYPHER = f"{EXPORTS}/graph.cypher"
"""neo4j data export file path"""

EXPORTS_STATEMENTS = f"{EXPORTS}/statements.csv"
"""complete sorted statements file path"""

JOBS = "jobs"
"""Job data prefix"""

JOB_RUNS = f"{JOBS}/runs"
"""Job runs result storage prefix"""


def job_prefix(name: str) -> str:
    return f"{JOB_RUNS}/{name}"


def job_run(name: str, run_id: str | None = None) -> str:
    return f"{job_prefix(name)}/{run_id or ensure_uuid()}.json"


QUEUE = "queue"
"""Base path for global CRUD action queue"""


def queue(tenant: str | None = TENANT) -> str:
    """
    Get the path for the global CRUD action queue.

    All lakehouse mutations (entity upsert/delete, file archive, etc.)
    go through this single queue, ordered by UUID7.

    Layout: queue/{tenant}/
    """
    return join_relpaths(QUEUE, tenant or TENANT)
