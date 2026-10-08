"""DuckDB SQL builders for the statement store.

All dedupe / fragment-supersession / grace logic is `_dedupe_sql`, run by reads
over dirty partitions (`dedupe_rows_sql`) and by merges (`build_merge_sql`); a
merged partition reads as a plain scan (`live_rows_sql`).
"""

import os
from datetime import datetime
from typing import Iterable

import pyarrow as pa
from anystore.util import ensure_uuid
from banal import ensure_list
from deltalake import DeltaTable
from ftmq.query import Query
from ftmq.query.leaves import IdLeaf
from ftmq.query.sql import PruneFn
from ftmq.store.lake import BUCKET_DOCUMENT, BUCKET_PAGE, TARGET_SIZE

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.shards import entity_shard, shard_hex_width
from ftm_lakehouse.model.statement import PA_TS, SHARDED_SCHEMA, TABLE_RAW
from ftm_lakehouse.util import parse_byte_size, validate_origin

SWEEP_BATCH_SIZE = 50_000
"""Rows per Arrow batch when a read materialises Python dicts (DuckDB's 1M
default is a million dicts at once)."""

SHARD_MIN_FILE_SIZE = 32 * 1_048_576
"""Floor for `shard_target_file_size`, so a re-shard does not explode the file count."""

MERGED_PREFIX = "merged-"
"""Basename prefix of `merge` output. A partition whose files all carry it is
clean (read without the dedupe); a ``part-*`` file makes it dirty."""

MERGE_COMMIT_BATCH = 64
"""Merged partitions per Delta commit – one commit each bloats the log."""

LARGE_BUCKETS = (BUCKET_DOCUMENT, BUCKET_PAGE)
"""Buckets holding full-text values – ftmq's ``WRITER_LARGE`` profile."""

TABLE_CONFIGURATION = {
    "delta.logRetentionDuration": "interval 1 days",
    "delta.deletedFileRetentionDuration": "interval 1 hours",
}
"""Delta table properties the store is created with (and migrated to), bounding
the log every reader replays – nothing time-travels."""

_DUCKDB_TYPES = {pa.string(): "VARCHAR", pa.bool_(): "BOOLEAN", PA_TS: "TIMESTAMPTZ"}
"""DuckDB type per `SHARDED_SCHEMA` arrow type."""

_FILE_COLUMNS = "SELECT {} WHERE false".format(
    ", ".join(
        f"NULL::{_DUCKDB_TYPES[f.type]} AS {f.name}"
        for f in SHARDED_SCHEMA
        if f.name not in ("shard", "bucket", "origin")
    )
)
"""Empty typed row set unioned with a partition's files, so a column they predate
reads as ``NULL``."""


def duckdb_config() -> dict[str, str]:
    """DuckDB config from the lakehouse settings – memory limit, spill and
    extension directories.

    Call once per DuckDB instance: each call gets its own spill subdirectory,
    as instances sharing one overwrite each other's spill files. No
    ``TimeZone`` – at connect time it installs ``icu`` (fails offline); sessions
    pin UTC after connecting.
    """
    settings = Settings()
    config: dict[str, str] = {"memory_limit": settings.duckdb_memory_limit}
    if settings.duckdb_temp_directory:
        # DuckDB creates the spill directory but not its parents
        os.makedirs(settings.duckdb_temp_directory, exist_ok=True)
        config["temp_directory"] = os.path.join(
            settings.duckdb_temp_directory, ensure_uuid()
        )
    if settings.duckdb_extension_directory:
        config["extension_directory"] = settings.duckdb_extension_directory
    return config


def worker_duckdb_config(workers: int) -> dict[str, str]:
    """[`duckdb_config`][duckdb_config] for one of ``workers`` processes, one call
    per task.

    Memory limit and threads are split between the workers.
    """
    config = duckdb_config()
    budget = parse_byte_size(config["memory_limit"])
    config["memory_limit"] = f"{budget // workers}B"
    config["threads"] = str(max((os.cpu_count() or 1) // workers, 1))
    return config


def _string_literal(value: str) -> str:
    """Escape ``value`` for interpolation as a single-quoted SQL literal."""
    return value.replace("'", "''")


def delta_scan_sql(table_uri: str) -> str:
    """``delta_scan('<uri>')``, the uri quote-escaped (no prepared parameters)."""
    return f"delta_scan('{_string_literal(table_uri)}')"


def raw_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the ``statement_raw`` view: every physical row,
    tombstones and duplicates included."""
    return f"SELECT * FROM {delta_scan_sql(dt.table_uri)}"


NONFRAGMENT_KEY = ("shard", "bucket", "origin", "entity_id", "id", "role")
"""Row identity of a non-fragment statement. ``entity_id`` is redundant with
``id`` but lets an ``entity_id`` filter push below the window."""

FRAGMENT_GROUP = ("shard", "bucket", "origin", "entity_id", "prop", "fragment", "role")
"""Supersession group of a fragment statement – the latest emission survives."""


def _dedupe_sql(
    source: str,
    where: str = "",
    tombstone: str = "deleted_at IS NULL",
    order_by: str = "",
    select: str = "*",
) -> str:
    """The dedupe query, shared by dirty-partition reads and `build_merge_sql`.

    - ``fragment = ''``: one row per `NONFRAGMENT_KEY`, latest ``last_seen``
      winning, ``deleted_at`` breaking the tie (a tombstone's ``last_seen`` is
      at least that of the row it shadows).
    - ``fragment != ''``: per `FRAGMENT_GROUP` the latest emission survives,
      one row per ``id``.

    NULL roles group together. ``first_seen`` folds to its minimum per ``id``
    and ``role`` (never across a fragment group), so a re-import is no diff
    change. ``tombstone`` filters the union.
    """

    key, group = ", ".join(NONFRAGMENT_KEY), ", ".join(FRAGMENT_GROUP)
    return f"""
WITH base AS (
    SELECT * FROM {source} {where}
),
nonfragment_rows AS (
    SELECT * REPLACE (
        MIN(first_seen) OVER (PARTITION BY {key}) AS first_seen
    )
    FROM base
    WHERE fragment = ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {key}
        ORDER BY last_seen DESC, deleted_at DESC NULLS LAST
    ) = 1
),
fragment_rows AS (
    SELECT * REPLACE (
        MIN(first_seen) OVER (PARTITION BY {group}, id) AS first_seen
    )
    FROM base
    WHERE fragment != ''
    QUALIFY last_seen = MAX(last_seen) OVER (PARTITION BY {group})
    AND ROW_NUMBER() OVER (
        PARTITION BY {group}, id
        ORDER BY last_seen DESC, deleted_at DESC NULLS LAST
    ) = 1
)
SELECT {select} FROM (
    SELECT * FROM nonfragment_rows
    UNION ALL
    SELECT * FROM fragment_rows
)
WHERE {tombstone}
{order_by}
""".strip()


def live_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the connection-level ``statement`` view over
    ``delta_scan`` (``stats()``, raw SQL) – always the dedupe."""
    return dedupe_rows_sql(delta_scan_sql(dt.table_uri))


def live_rows_sql(source: str) -> str:
    """Live rows of a **clean** ``source`` – a filtered scan. ``canonical_id`` is
    ``entity_id``, for ftmq's query layer."""
    return f"SELECT *, entity_id AS canonical_id FROM {source} WHERE deleted_at IS NULL"


def dedupe_rows_sql(source: str) -> str:
    """Live rows of a ``source`` that may hold un-merged files: `_dedupe_sql`,
    ``canonical_id`` as in [`live_rows_sql`][live_rows_sql]. Row for row what
    the same partition reads once merged."""
    return _dedupe_sql(source, select="*, entity_id AS canonical_id")


def build_merge_sql(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
    source: str = TABLE_RAW.name,
    select: str = "*",
) -> str:
    """`_dedupe_sql` collapsing one partition for a merge, ordered by the file
    sort key. Tombstones within grace survive to keep shadowing.

    Args:
        shard: The partition's shard.
        bucket: The partition's bucket.
        origin: The partition's origin – validated before interpolation.
        grace_cutoff: Tombstones with ``deleted_at <= grace_cutoff`` are dropped.
        source: Relation holding the rows – ``statement_raw``, or one
            partition's files ([`partition_source_sql`][partition_source_sql]).
        select: Projection of the output, e.g. without the partition columns.
    """
    origin = validate_origin(origin)
    return _dedupe_sql(
        source=source,
        where=f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'",
        tombstone=(
            "(deleted_at IS NULL OR deleted_at > "
            f"TIMESTAMPTZ '{grace_cutoff.isoformat()}')"
        ),
        order_by="ORDER BY entity_id, fragment, role, prop, id, last_seen DESC",
        select=select,
    )


def read_parquet_sql(files: Iterable[str]) -> str:
    """``read_parquet`` over ``files``, unioned by name. No hive partitioning:
    it would parse a shard like ``10`` as an integer."""
    paths = ", ".join(f"'{_string_literal(f)}'" for f in files)
    return f"read_parquet([{paths}], union_by_name = true, hive_partitioning = false)"


def partition_source_sql(
    files: Iterable[str], shard: str, bucket: str, origin: str
) -> str:
    """One partition's files as a `SHARDED_SCHEMA` relation, read without
    ``delta_scan``.

    Partition columns come in as constants, columns the files predate read as
    ``NULL`` and ``fragment`` is coalesced to ``''``. ``origin`` is validated
    before interpolation.
    """
    origin = validate_origin(origin)
    constants = {"shard": shard, "bucket": bucket, "origin": origin}
    projection = ", ".join(
        (
            f"'{_string_literal(constants[f.name])}' AS {f.name}"
            if f.name in constants
            else (
                "COALESCE(fragment, '') AS fragment" if f.name == "fragment" else f.name
            )
        )
        for f in SHARDED_SCHEMA
    )
    return (
        f"(SELECT {projection} FROM ("
        f"SELECT * FROM {read_parquet_sql(files)} UNION ALL BY NAME {_FILE_COLUMNS}))"
    )


def merge_copy_options(bucket: str) -> str:
    """DuckDB ``COPY`` options for a merged ``bucket`` file: zstd level 3,
    10k-row groups for the full-text buckets, 100k otherwise. ``RETURN_STATS``
    yields what the Delta ``add`` action needs."""
    rows = 10_000 if bucket in LARGE_BUCKETS else 100_000
    return (
        "FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, "
        f"ROW_GROUP_SIZE {rows}, RETURN_STATS"
    )


def shard_expr_sql(shards: int, column: str = "entity_id") -> str:
    """DuckDB expression for ``column``'s shard key – the SQL twin of
    `entity_shard`, pinned to agree by ``test_shard_expr_sql_parity``."""
    if shards <= 1:
        return "'0'"
    width = shard_hex_width(shards)
    return (
        f"printf('%0{width}x', "
        f"(('0x' || substr(sha1({column}), 1, 8))::BIGINT) % {int(shards)})"
    )


def build_shard_sql(
    shard: str, bucket: str, origin: str, shards: int, source: str = TABLE_RAW.name
) -> str:
    """One partition's raw rows with ``shard`` recomputed for ``shards`` shards.

    Unordered and un-deduped – the follow-up merge does both. ``origin`` is
    validated before interpolation.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT * REPLACE ({shard_expr_sql(shards)} AS shard) "
        f"FROM {source} "
        f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
    )


def shard_target_file_size(shards: int) -> int:
    """Delta ``target_file_size`` for a re-shard: `TARGET_SIZE` split across the
    shards (a writer each is open at once), at least `SHARD_MIN_FILE_SIZE`."""
    return max(TARGET_SIZE // max(shards, 1), SHARD_MIN_FILE_SIZE)


def make_prune_by_shard(shards: int = 0) -> PruneFn:
    """Prune function mapping a query's entity ids to their shards."""

    def prune(q: Query) -> set[str]:
        values: set[str] = set()
        for f in q._leaves:
            if isinstance(f, IdLeaf):
                if f.comparator in ("eq", "in"):
                    for v in ensure_list(f.value):
                        values.add(entity_shard(v, shards))
        return values

    return prune
