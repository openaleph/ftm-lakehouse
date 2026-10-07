"""DuckDB SQL builders for the statement store.

All dedupe / fragment-supersession / grace logic is `_dedupe_sql`: a read over a
partition holding files a merge did not write runs it
([`dedupe_rows_sql`][dedupe_rows_sql]), a merged partition is a plain scan
([`live_rows_sql`][live_rows_sql]), and a merge runs it over one partition's
files ([`build_merge_sql`][build_merge_sql]).
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
"""Rows per Arrow batch when a read materialises them as Python dicts –
DuckDB's default of 1M would hold a million dicts at once."""

SHARD_MIN_FILE_SIZE = 32 * 1_048_576  # 32 MB
"""Floor for `shard_target_file_size`, so a re-shard does not explode the file count."""

MERGED_PREFIX = "merged-"
"""Basename prefix of the files `ParquetStore.merge` writes. A partition whose
files all carry it is canonical and read without the dedupe; any other file
(``part-*`` from appends and re-shards) makes it dirty."""

FALLBACK_MEMORY_LIMIT = "8GB"
"""Per-worker budget when ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT`` is no byte size
(e.g. ``80%``)."""

MERGE_COMMIT_BATCH = 64
"""Merged partitions per Delta commit – a commit per partition made the log
and its checkpoints the bottleneck of a merge."""

LARGE_BUCKETS = (BUCKET_DOCUMENT, BUCKET_PAGE)
"""Buckets holding full-text values – ftmq's ``WRITER_LARGE`` profile."""

TABLE_CONFIGURATION = {
    "delta.logRetentionDuration": "interval 1 days",
    "delta.deletedFileRetentionDuration": "interval 1 hours",
}
"""Delta table properties the store is created with (and migrated to).

Both bound the log every reader replays: ``remove`` actions leave checkpoints
after an hour (``vacuum`` runs at zero retention anyway), superseded log
entries go after a day (nothing time-travels)."""

_DUCKDB_TYPES = {pa.string(): "VARCHAR", pa.bool_(): "BOOLEAN", PA_TS: "TIMESTAMPTZ"}
"""DuckDB type per `SHARDED_SCHEMA` arrow type."""

_FILE_COLUMNS = "SELECT {} WHERE false".format(
    ", ".join(
        f"NULL::{_DUCKDB_TYPES[f.type]} AS {f.name}"
        for f in SHARDED_SCHEMA
        if f.name not in ("shard", "bucket", "origin")
    )
)
"""Empty, fully typed row set unioned by name with a partition's files, so a
column they predate reads as ``NULL``."""


def duckdb_config() -> dict[str, str]:
    """DuckDB config from the lakehouse settings – memory limit, spill and
    extension directories.

    One call per DuckDB instance: each call spills into its own subdirectory of
    ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY`` – instances number their spill files
    alike, so two sharing a directory overwrite each other's blocks. DuckDB
    creates the subdirectory on its first spill and removes it on close.

    No ``TimeZone``: as a connect-time option it makes DuckDB install ``icu``,
    which fails offline; the session is pinned to UTC after connecting instead.
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
    """[`duckdb_config`][duckdb_config] for one of ``workers`` processes – one
    call per task, as each task connects its own instance.

    Memory limit and threads are split between them, so the limit stays the
    ceiling for the whole operation. A limit that is no byte size falls back
    to `FALLBACK_MEMORY_LIMIT`.
    """
    config = duckdb_config()
    try:
        budget = parse_byte_size(config["memory_limit"])
    except ValueError:
        budget = parse_byte_size(FALLBACK_MEMORY_LIMIT)
    config["memory_limit"] = f"{budget // max(workers, 1)}B"
    config["threads"] = str(max((os.cpu_count() or 1) // max(workers, 1), 1))
    return config


def split_duckdb_config(config: dict[str, str], parts: int) -> list[dict[str, str]]:
    """``config`` shared out between ``parts`` DuckDB instances of one task –
    memory limit and threads divided, each instance spilling beside the task's
    directory rather than into it (instances number their spill files alike)."""
    if parts <= 1:
        return [config]
    shares = []
    for i in range(parts):
        share = dict(config)
        if "memory_limit" in config:
            limit = parse_byte_size(config["memory_limit"])
            share["memory_limit"] = f"{limit // parts}B"
        if "threads" in config:
            share["threads"] = str(max(int(config["threads"]) // parts, 1))
        if "temp_directory" in config:
            share["temp_directory"] = f"{config['temp_directory']}-{i}"
        shares.append(share)
    return shares


def _string_literal(value: str) -> str:
    """Escape ``value`` for interpolation as a single-quoted SQL literal."""
    return value.replace("'", "''")


def delta_scan_sql(table_uri: str) -> str:
    """``delta_scan('<uri>')``, the uri quote-escaped – ``delta_scan`` takes no
    prepared parameters."""
    return f"delta_scan('{_string_literal(table_uri)}')"


def raw_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the ``statement_raw`` view: every physical row,
    tombstones and duplicates included."""
    return f"SELECT * FROM {delta_scan_sql(dt.table_uri)}"


NONFRAGMENT_KEY = ("shard", "bucket", "origin", "entity_id", "id", "role")
"""Row identity of a non-fragment statement. ``entity_id`` is implied by ``id``
but lets an ``entity_id = ?`` filter push below the window."""

FRAGMENT_GROUP = ("shard", "bucket", "origin", "entity_id", "prop", "fragment", "role")
"""Supersession group of a fragment statement – the latest emission survives."""


def _dedupe_sql(
    source: str,
    where: str = "",
    tombstone: str = "deleted_at IS NULL",
    order_by: str = "",
    select: str = "*",
) -> str:
    """The dedupe query – reads over a dirty partition and
    [`build_merge_sql`][build_merge_sql] alike.

    Rows split on ``fragment`` before any window runs:

    - ``fragment = ''``: one row per `NONFRAGMENT_KEY`, the latest ``last_seen``
      winning. A tombstone's ``last_seen`` is bumped at write time to at least
      that of the row it shadows; ``deleted_at`` breaks the tie.
    - ``fragment != ''``: per `FRAGMENT_GROUP` only the latest emission
      survives – all of its rows, one per ``id``.

    ``origin`` and ``role`` are in every key: the same content from two origins
    or two roles stays two rows (NULL roles group together). ``first_seen``
    folds to its minimum per ``id`` and ``role`` – never across a fragment
    group, whose rows are different values – so a re-import does not read as a
    change to the export diff. ``tombstone`` filters the union, ``select`` and
    ``order_by`` shape it.
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
    ``delta_scan`` (``stats()``, raw SQL) – always reconciling, having no
    partition to ask whether it is clean."""
    return dedupe_rows_sql(delta_scan_sql(dt.table_uri))


def live_rows_sql(source: str) -> str:
    """Live rows of a **clean** ``source`` – merge output is canonical, so a
    filtered scan. ``canonical_id`` is synthesised from ``entity_id`` for
    ftmq's query layer (one dataset, no entity resolution)."""
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
    """`_dedupe_sql` collapsing one partition for a merge – over the raw rows,
    so tombstones within grace survive to keep shadowing, ordered by the file
    sort key.

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
    """One partition's files as a `SHARDED_SCHEMA` relation – what reads and
    merges use instead of ``delta_scan``, which replays the log per query.

    The partition columns come in as constants, a column the files predate
    reads as ``NULL`` (`_FILE_COLUMNS`), and ``fragment`` is coalesced to its
    ``''`` sentinel. ``origin`` is validated before interpolation.
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
    10k-row groups for the full-text buckets and 100k otherwise (ftmq's 1M
    keeps the encoding on one core). ``RETURN_STATS`` yields what the Delta
    ``add`` action needs."""
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

    Unordered and un-deduped: a re-shard moves rows, the follow-up merge sorts.
    ``origin`` is validated before interpolation.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT * REPLACE ({shard_expr_sql(shards)} AS shard) "
        f"FROM {source} "
        f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
    )


def shard_target_file_size(shards: int) -> int:
    """Delta ``target_file_size`` for a re-shard: a writer per target shard is
    open at once, so `TARGET_SIZE` is divided by the shard count to bound
    memory – never below `SHARD_MIN_FILE_SIZE`."""
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
