"""Pure functions for Delta Lake parquet operations.

DuckDB SQL builders for the statement store's reads and for the per-partition
merge. All dedupe / fragment-supersession / grace logic lives in one place –
`_dedupe_sql` – and serves both: a read over a partition that holds files a
merge did not write reconciles them ([`dedupe_rows_sql`][dedupe_rows_sql]),
while a partition made of merge output alone is canonical by construction
and gets a plain scan ([`live_rows_sql`][live_rows_sql]). A merge is the same
query over one partition's files
([`build_merge_sql`][build_merge_sql], [`partition_source_sql`][partition_source_sql]),
written back with DuckDB's ``COPY`` ([`merge_copy_options`][merge_copy_options])
– physical compaction, never a precondition for reading. ``statement_raw``
exposes every underlying row, tombstones and duplicates included, for the
paths that need them (``merge``, ``get_entity_ids`` over the raw source).
See `_dedupe_sql` for the two-branch fragment semantics.
"""

import math
import os
from datetime import datetime
from typing import Iterable

import pyarrow as pa
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
"""Rows per Arrow batch when `ParquetStore.sweep` materialises them as
Python dicts. DuckDB's own default (1M) is sized for a columnar consumer;
turning a batch that size into dicts would hold a million of them at once, so
the fused export asks for a smaller one. Bounds rows in flight, not bytes
scanned – the scan stays streaming either way."""

MERGE_SPILL_FACTOR = 32
"""Estimated peak DuckDB footprint of the merge pipeline per compressed
parquet byte – zstd/dictionary decompression blow-up (5–20x on statement
data) times the concurrent sort materialisations of `_dedupe_sql`
(window groups + final ``ORDER BY``), padded for headroom. Used by
`merge_slice_count` to bound each merge slice to the configured
DuckDB memory limit."""

MERGE_SAMPLE_SIZE = 10_000
"""Reservoir sample size for `build_bounds_sample_sql`. Bounds the
slice-boundary resolution – boundary quality only affects load balance
across slices, never correctness, so a fixed sample is fine."""

SHARD_MIN_FILE_SIZE = 32 * 1_048_576  # 32 MB
"""Floor for `shard_target_file_size` – below this a re-shard would
trade its memory bound for a file-count explosion the follow-up ``merge``
has to rewrite."""

MERGED_PREFIX = "merged-"
"""Basename prefix of the data files `ParquetStore.merge` writes. A partition
whose active files all carry it is canonical – one row per key, supersession
applied, timestamps folded – so a read over it needs no reconciling; any
other file (delta-rs appends and the re-shard write ``part-*``) makes the
partition dirty. The signal lives in the Delta snapshot's file list, so it
costs no tag I/O and cannot drift from the data."""

FALLBACK_MEMORY_LIMIT = "8GB"
"""Slice budget when ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT`` is not a parseable
byte size (e.g. a DuckDB percentage limit) – mirrors the conservative
[`Settings`][ftm_lakehouse.core.settings.Settings] default."""

MERGE_COMMIT_BATCH = 64
"""Merged partitions per Delta commit. Every commit is a log entry, and every
hundredth one a checkpoint that rewrites the table's whole file list – on a
large store that is gigabytes – so one commit per partition made the log the
bottleneck of a merge. Batching also bounds what a failed run leaves behind:
committed batches stay merged, the rest are orphans the next ``vacuum``
removes."""

LARGE_BUCKETS = (BUCKET_DOCUMENT, BUCKET_PAGE)
"""Buckets holding full-text values – ftmq's ``WRITER_LARGE`` profile."""

TABLE_CONFIGURATION = {
    "delta.logRetentionDuration": "interval 1 days",
    "delta.deletedFileRetentionDuration": "interval 1 hours",
}
"""Delta table properties the store is created with (and migrated to).

Both bound the size of the transaction log, which every reader and writer
replays from its latest checkpoint:

- ``deletedFileRetentionDuration`` is how long a ``remove`` action stays in
  checkpoints. A merge removes every file of the partitions it rewrites, so at
  the Delta default of a week the checkpoints carry the removes of every
  rewrite that week next to the live files. ``vacuum`` runs with a zero
  retention already, so nothing reads these.
- ``logRetentionDuration`` is how long superseded commits and checkpoints stay
  on disk. Nothing time-travels – diff states record a version number, they
  never load one – so the Delta default of 30 days only kept dead checkpoints
  around."""

_DUCKDB_TYPES = {pa.string(): "VARCHAR", pa.bool_(): "BOOLEAN", PA_TS: "TIMESTAMPTZ"}
"""DuckDB type per `SHARDED_SCHEMA` arrow type."""

_FILE_COLUMNS = "SELECT {} WHERE false".format(
    ", ".join(
        f"NULL::{_DUCKDB_TYPES[f.type]} AS {f.name}"
        for f in SHARDED_SCHEMA
        if f.name not in ("shard", "bucket", "origin")
    )
)
"""An empty row set carrying every column a data file can hold, typed –
unioned by name with a partition's files (`partition_source_sql`) so a column
they predate reads as ``NULL``."""


def duckdb_config() -> dict[str, str]:
    """LakeStore DuckDB config derived from lakehouse settings.

    Deliberately no ``TimeZone``: as a connect-time option it is applied
    before the statically linked ``icu`` registers, so DuckDB tries to
    install ``icu`` – which fails offline or against a read-only
    ``extension_directory``. `LakeStore` pins the session to UTC after
    connecting instead (``LOAD icu; SET GLOBAL TimeZone='UTC'``).

    Per-query memory is bounded by `Settings.duckdb_memory_limit`
    (env: ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``, default ``8GB``); queries
    exceeding the limit spill to `Settings.duckdb_temp_directory`
    (env: ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY``), which defaults to
    ``{OS temp dir}/duckdb`` – DuckDB's own default is ``.tmp`` relative to
    the working directory. Extensions (notably
    ``delta``) are loaded from `Settings.duckdb_extension_directory`
    (env: ``LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY``) when set, otherwise
    from ``$HOME/.duckdb/extensions``. Passed to
    `LakeStore` via the ``duckdb_config`` kwarg.
    """
    settings = Settings()
    config: dict[str, str] = {"memory_limit": settings.duckdb_memory_limit}
    if settings.duckdb_temp_directory:
        config["temp_directory"] = settings.duckdb_temp_directory
    if settings.duckdb_extension_directory:
        config["extension_directory"] = settings.duckdb_extension_directory
    return config


def _string_literal(value: str) -> str:
    """Escape ``value`` for interpolation as a single-quoted SQL literal."""
    return value.replace("'", "''")


def delta_scan_sql(table_uri: str) -> str:
    """``delta_scan('<uri>')`` with the URI single-quote–escaped.

    DuckDB's ``delta_scan`` does not accept prepared parameters for its
    URI argument, so the URI is interpolated as a SQL string literal.
    Single quotes are doubled to prevent injection if a future code
    path lets a dataset name (and thus the URI) carry a quote – primary
    validation is in `validate_dataset_name`.
    """
    return f"delta_scan('{_string_literal(table_uri)}')"


def raw_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the ``statement_raw`` view.

    Surfaces every physical row in the Delta table, including
    tombstones and pre-merge duplicates. Used by [`build_merge_sql`][build_merge_sql]
    and raw-source queries (diff exports) – any path that needs the
    physical layout visible.
    """
    return f"SELECT * FROM {delta_scan_sql(dt.table_uri)}"


NONFRAGMENT_KEY = ("shard", "bucket", "origin", "entity_id", "id", "role")
"""Row identity of a non-fragment statement – one survivor per key.

``entity_id`` is redundant – a statement id is content-hashed over its entity,
so it belongs to exactly one – but naming it lets DuckDB push an
``entity_id = ?`` filter below the window instead of resolving every
duplicate group of the partition for one lookup."""

FRAGMENT_GROUP = ("shard", "bucket", "origin", "entity_id", "prop", "fragment", "role")
"""Supersession group of a fragment statement – the latest emission survives."""


def _dedupe_sql(
    source: str,
    where: str = "",
    tombstone: str = "deleted_at IS NULL",
    order_by: str = "",
    select: str = "*",
) -> str:
    """Two-branch dedupe skeleton – reads over a dirty partition and
    [`build_merge_sql`][build_merge_sql] alike.

    Rows route into two isolated branches on ``fragment`` (empty-string
    sentinel, applied *before* any window runs so the branches can never
    group with each other):

    - **non-fragment** (``fragment = ''``): at most one row per statement
      ``id`` per ``(origin, role)`` – ``QUALIFY ROW_NUMBER() OVER (...
      ORDER BY last_seen DESC, deleted_at DESC NULLS LAST) = 1`` picks the
      row with the latest ``last_seen``. Entity ids (and therefore statement
      ids) are uniquely placed in one ``(shard, bucket)`` by the model
      layer; ``origin`` in the key keeps the same id under two origins as
      two independent rows (matching the per-``(shard, bucket, origin)``
      scope of physical merge – load-bearing when the source spans
      origins, as a read over a ``(shard, bucket)`` pair does). Tombstones bump ``last_seen``
      to ``MAX(deleted_at, the shadowed row's last_seen)`` at write time,
      so the tombstone can never rank below the row it deletes – the
      ``deleted_at`` tiebreak resolves the tie that leaves, and the one a
      delete and an emission sharing a second would produce – and the
      ``tombstone`` predicate decides whether it survives the final
      projection.
    - **fragment-bearing** (``fragment != ''``): supersession per
      ``(origin, entity_id, prop, fragment, role)`` group – ``QUALIFY
      last_seen = MAX(last_seen) OVER (...)`` admits the rows tied at
      the group's maximum ``last_seen`` (multi-valued props of one
      emission share their timestamp and survive together) and drops
      earlier emissions; among the tied rows the ANDed ``ROW_NUMBER``
      keeps one row per statement ``id``, so physically identical
      duplicates (a re-import of the same data) collapse and the merge
      is idempotent. ``origin`` in the group key keeps the same fragment
      under two origins as two independent supersession groups.

    ``role`` sits in every window key of both branches, which is what makes
    it the fourth row-identity dimension after ``id`` / ``origin`` /
    ``fragment``: two roles asserting identical content survive as two rows
    (full provenance) while one role re-asserting collapses. ``role`` is
    nullable, and DuckDB groups NULLs together in a ``PARTITION BY``, so
    role-less rows dedupe against each other rather than each surviving
    alone.

    Both branches fold ``first_seen`` to ``MIN(first_seen)`` over the rows
    carrying the same *content asserted by the same role* – ``(id, role)`` –
    via ``SELECT * REPLACE``, so re-importing identical data keeps its
    original observation date and does not read as a change (windows compute
    before ``QUALIFY`` filters, so dropped duplicates still contribute their
    timestamps). ``role`` belongs in the fold key for the same reason it
    belongs in the identity key: a role's *first* assertion of content an
    older role already wrote is a new row, and folding it onto the older
    row's date would both misdate it and hide it from the export sweep's
    diff, which detects change on ``first_seen``.

    The fragment branch folds by ``id`` too, not by its supersession group:
    the group spans *different* values, so folding across it would stamp a
    superseding value with the date of the row it replaced – both a false
    ``first_seen`` and, because ``first_seen`` is what the export sweep's
    diff detects change with, a silently undiffable update. Only the ``QUALIFY``
    windows below work at group scope, which is what supersession means.

    Every filter column a read pushes down sits in the window keys:
    ``shard`` / ``bucket`` / ``origin`` as partition keys and ``entity_id``
    in both branches, so an ``entity_id = ?`` lookup is evaluated below the
    windows – on the files' statistics – instead of resolving every group of
    the partition first. Any other predicate waits above them, as it must.

    Args:
        source: Relation to read from – a ``delta_scan('...')`` clause,
            a view name or a parenthesised subquery.
        where: Optional ``WHERE ...`` clause scoping ``source``.
        tombstone: Tombstone predicate applied after the branches union.
        order_by: Optional ``ORDER BY ...`` clause on the final output.
        select: Projection of the final output – ``ORDER BY`` sits on the same
            level, so a narrowed projection keeps the order.

    Returns:
        Executable DuckDB SQL.
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
    """SELECT body for the connection-level ``statement`` view.

    The view ftmq's ``LakeStore`` registers over ``delta_scan`` – what
    ``stats()``, the raw-SQL CLI and nothing else read. Partition-scoped reads
    build their own view per cursor over the partition's files and choose
    between [`live_rows_sql`][live_rows_sql] and [`dedupe_rows_sql`][dedupe_rows_sql]
    by whether the partition is clean; this one has no partition to ask, so it
    always reconciles.
    """
    return dedupe_rows_sql(delta_scan_sql(dt.table_uri))


def live_rows_sql(source: str) -> str:
    """The live ``statement`` rows of a **clean** ``source``.

    A partition made of merge output alone is canonical – one row per key,
    supersession applied, ``first_seen`` / ``last_seen`` folded – so its live
    rows are the non-tombstoned physical rows: a plain filtered scan, no
    window function, and every ``schema`` / ``prop`` / ``entity_id`` filter
    reaches the files' statistics.

    ``canonical_id`` is not stored – this is a single-dataset store with no
    entity resolution, so it always equals ``entity_id`` – and is synthesised
    as ``entity_id AS canonical_id`` so ftmq's query layer (which keys entity
    identity on ``canonical_id``) resolves against the view unchanged.
    [`raw_view_sql`][raw_view_sql] deliberately omits it so ``merge`` never
    materialises the duplicate column.
    """
    return f"SELECT *, entity_id AS canonical_id FROM {source} WHERE deleted_at IS NULL"


def dedupe_rows_sql(source: str) -> str:
    """The live ``statement`` rows of a ``source`` that may hold un-merged
    files: `_dedupe_sql` – duplicates collapsed, supersession
    applied, timestamps folded, tombstones hidden – with ``canonical_id``
    synthesised as in [`live_rows_sql`][live_rows_sql]. Row for row what a
    read over the partition's merge output returns, which is what makes
    ``merge`` an optimisation rather than a precondition."""
    return _dedupe_sql(source, select="*, entity_id AS canonical_id")


def build_merge_sql(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
    entity_id_range: tuple[str | None, str | None] = (None, None),
    source: str = TABLE_RAW.name,
    select: str = "*",
) -> str:
    """DuckDB SQL that collapses one partition for physical merge.

    `_dedupe_sql` over the **raw** ``statement_raw`` view (not the
    deduped ``statement``) because ``merge`` needs every row visible –
    including tombstones within the grace window, which must persist
    physically to keep shadowing their live rows – scoped to one
    ``(shard, bucket, origin)`` partition. Output is ordered by
    ``(entity_id, fragment, role, prop, id, last_seen DESC)`` – the file sort
    key – so the rewritten parquet file is ready for future merges
    without re-sort.

    Args:
        shard: Target shard value (hex-padded).
        bucket: Target bucket (``thing`` / ``interval`` / ``document`` /
            ``page`` / ``pages`` / ``mention``).
        origin: Target origin tag – re-validated here, so it is safe to
            interpolate: `validate_origin` rejects quote characters.
        grace_cutoff: Tombstones with ``deleted_at <= grace_cutoff`` are
            dropped. Typically ``now - LAKEHOUSE_GRACE_PERIOD_DAYS``.
        entity_id_range: Optional half-open ``[lo, hi)`` bound on
            ``entity_id`` (``None`` = unbounded on that side) scoping the
            merge to one range slice (`slice_ranges`). Every dedupe
            group is a function of a single entity – the non-fragment key
            ends in the statement ``id`` (owned by exactly one entity),
            the fragment key contains ``entity_id`` itself – so an
            ``entity_id`` predicate can never split a group.
        source: Relation holding the partition's rows – the ``statement_raw``
            view by default, or one partition's files
            ([`partition_source_sql`][partition_source_sql]).
        select: Projection of the output, e.g. without the partition columns
            a data file must not carry.

    Returns:
        Executable DuckDB SQL.
    """
    origin = validate_origin(origin)
    lo, hi = entity_id_range
    where = f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
    if lo is not None:
        where += f" AND entity_id >= '{_string_literal(lo)}'"
    if hi is not None:
        where += f" AND entity_id < '{_string_literal(hi)}'"
    return _dedupe_sql(
        source=source,
        where=where,
        tombstone=(
            "(deleted_at IS NULL OR deleted_at > "
            f"TIMESTAMPTZ '{grace_cutoff.isoformat()}')"
        ),
        order_by="ORDER BY entity_id, fragment, role, prop, id, last_seen DESC",
        select=select,
    )


def read_parquet_sql(files: Iterable[str]) -> str:
    """``read_parquet`` over ``files``, unioned by column name.

    Hive path parsing stays off: Delta data files carry no partition columns,
    and parsing them from the path would turn a shard like ``10`` into an
    integer.
    """
    paths = ", ".join(f"'{_string_literal(f)}'" for f in files)
    return f"read_parquet([{paths}], union_by_name = true, hive_partitioning = false)"


def partition_source_sql(
    files: Iterable[str], shard: str, bucket: str, origin: str
) -> str:
    """One partition's parquet files as a relation shaped like `SHARDED_SCHEMA`.

    What reads and merges use instead of ``delta_scan``, which replays the
    Delta log on every query – on a store with a large log that costs more
    than the query does. The caller holds the snapshot and hands over the
    partition's files from it.

    Delta data files carry no partition columns, so ``shard`` / ``bucket`` /
    ``origin`` come in as constants. A column the files predate (one added by
    [`evolve_schema`][ftm_lakehouse.storage.parquet.ParquetStore.evolve_schema])
    reads as ``NULL``, as ``delta_scan`` would read it: the files are unioned
    *by name* with an empty, fully typed row set (`_FILE_COLUMNS`), so every
    column exists whatever the files carry – without opening them first to
    find out. ``fragment`` is the exception: its "no fragment" sentinel is
    the empty string, and `_dedupe_sql` routes on ``fragment = ''`` /
    ``!= ''`` – a NULL would fall out of both branches – so it is coalesced.

    Args:
        files: The partition's data files, readable by DuckDB.
        shard: The partition's shard.
        bucket: The partition's bucket.
        origin: The partition's origin – re-validated before interpolation.

    Returns:
        A parenthesised DuckDB subquery.
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
    """DuckDB ``COPY`` options for a merged data file of ``bucket``.

    The counterpart of ftmq's ``writer_for_bucket`` profiles: zstd level 3,
    10k-row groups for the full-text buckets. The other buckets get 100k-row
    groups instead of ftmq's 1M – DuckDB encodes row groups in parallel, and
    a million-row group keeps a merged ``mention`` file on one core. Bloom
    filters come with dictionary encoding, as in ftmq's profiles.
    ``RETURN_STATS`` reports the row count and file size a Delta ``add``
    action needs, from the writer rather than a second look at the file.
    """
    rows = 10_000 if bucket in LARGE_BUCKETS else 100_000
    return (
        "FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, "
        f"ROW_GROUP_SIZE {rows}, RETURN_STATS"
    )


def merge_duckdb_config(workers: int) -> dict[str, str]:
    """[`duckdb_config`][duckdb_config] for one of ``workers`` merge processes.

    Each worker is its own DuckDB instance, so the memory limit and the
    threads are split between them – ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT`` stays
    the ceiling for the whole merge, not per worker. A limit that is not a
    byte size (a percentage) falls back to `FALLBACK_MEMORY_LIMIT`, as
    ``merge_slice_count`` does.
    """
    config = duckdb_config()
    try:
        budget = parse_byte_size(config["memory_limit"])
    except ValueError:
        budget = parse_byte_size(FALLBACK_MEMORY_LIMIT)
    config["memory_limit"] = f"{budget // max(workers, 1)}B"
    config["threads"] = str(max((os.cpu_count() or 1) // max(workers, 1), 1))
    return config


def shard_expr_sql(shards: int, column: str = "entity_id") -> str:
    """DuckDB expression computing the shard key of ``column``.

    The SQL twin of `helpers.shards.entity_shard`,
    used by [`build_shard_sql`][build_shard_sql] so a re-shard recomputes every row's
    partition inside DuckDB's vectorised pipeline instead of marshaling
    ids into Python. ``banal.hash_data`` of a ``str`` is a plain SHA-1 of
    its UTF-8 bytes, which is exactly what DuckDB's ``sha1()`` returns –
    the two are pinned to agree by
    ``tests/test_logic_parquet.py::test_shard_expr_sql_parity``.

    Args:
        shards: Target shard count; ``<= 1`` collapses to the constant
            single-shard key, matching ``entity_shard``.
        column: Column holding the entity id.

    Returns:
        A DuckDB scalar expression yielding the hex-padded shard key.
    """
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
    """DuckDB SQL re-keying one partition's rows onto ``shards`` shards.

    ``SELECT *`` over the partition's **raw** rows (tombstones and
    pre-merge duplicates included – a re-shard moves rows, it does not
    decide what survives) with the stored ``shard`` swapped for the one
    [`shard_expr_sql`][shard_expr_sql] computes from ``entity_id``. ``REPLACE`` keeps
    the projection positional, so the result still *is*
    `SHARDED_SCHEMA` and streams
    straight into ``write_deltalake``.

    Deliberately unordered and un-deduped: the caller
    ([`shard`][ftm_lakehouse.storage.parquet.ParquetStore.shard]) re-stamps
    every rewritten partition as dirty, so the next ``merge`` restores the
    file sort order – paying for a sort here would only make the rewrite
    slower.

    Args:
        shard: Source shard value (hex-padded) to read.
        bucket: Source bucket – invariant under re-sharding.
        origin: Source origin tag – invariant under re-sharding.
            Re-validated here, so it is safe to interpolate.
        shards: Target shard count.
        source: Relation holding the partition's rows – the ``statement_raw``
            view by default, or one partition's files
            ([`partition_source_sql`][partition_source_sql]).

    Returns:
        Executable DuckDB SQL.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT * REPLACE ({shard_expr_sql(shards)} AS shard) "
        f"FROM {source} "
        f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
    )


def build_bounds_sample_sql(
    shard: str,
    bucket: str,
    origin: str,
    size: int = MERGE_SAMPLE_SIZE,
    source: str = TABLE_RAW.name,
) -> str:
    """DuckDB SQL reservoir-sampling ``entity_id`` values from one partition.

    Feeds `slice_ranges` with boundary candidates for a range-sliced
    merge. The partition filter sits in a subquery because DuckDB applies
    a query-level ``USING SAMPLE`` *before* the ``WHERE`` clause – sampled
    directly, most of the sample would come from other partitions.

    The reservoir draw is random, so slice boundaries vary between runs –
    that only shifts load balance across slices, never the merged output.

    Args:
        shard: Target shard value (hex-padded).
        bucket: Target bucket.
        origin: Target origin tag.
        size: Number of rows to sample.
        source: Relation holding the partition's rows, as for
            [`build_merge_sql`][build_merge_sql].

    Returns:
        Executable DuckDB SQL yielding one ``entity_id`` column.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT entity_id FROM ("
        f"SELECT entity_id FROM {source} "
        f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
        f") USING SAMPLE reservoir({int(size)} ROWS)"
    )


def slice_ranges(sample: list[str], slices: int) -> list[tuple[str | None, str | None]]:
    """Derive contiguous half-open ``entity_id`` ranges from a sample.

    Sorts ``sample`` and picks boundaries at even ranks, so ranges carry
    roughly equal row counts (entities with many statements are
    proportionally represented in the sample – weighting by row count is
    exactly what balances the sort windows). Ranges tile the full key
    space: the first is unbounded below, the last unbounded above, and
    consecutive ranges share their boundary (``hi`` of one is ``lo`` of
    the next), so every entity falls in exactly one range regardless of
    boundary quality. Duplicate boundaries (skewed sample) collapse, so
    fewer than ``slices`` ranges may come back.

    Python string sort order matches DuckDB's binary ``VARCHAR``
    comparison (UTF-8 byte order preserves code-point order), so the
    ranges partition exactly as the SQL predicates will.

    Args:
        sample: ``entity_id`` values drawn from the partition
            (`build_bounds_sample_sql`).
        slices: Desired number of ranges; clamped to the sample size.

    Returns:
        List of ``(lo, hi)`` bounds in ascending order, ``None`` for
        unbounded. ``[(None, None)]`` when no slicing is possible.
    """
    if slices <= 1 or not sample:
        return [(None, None)]
    ordered = sorted(sample)
    slices = min(slices, len(ordered))
    bounds: list[str] = []
    for i in range(1, slices):
        bound = ordered[i * len(ordered) // slices]
        if not bounds or bound > bounds[-1]:
            bounds.append(bound)
    return list(zip([None, *bounds], [*bounds, None]))


def merge_slice_count(partition_bytes: int, memory_limit: str) -> int:
    """Number of range slices to merge a partition of ``partition_bytes``.

    Estimates the peak DuckDB footprint of the merge pipeline as
    `MERGE_SPILL_FACTOR` times the partition's compressed parquet
    size and slices so each slice's estimate fits within ``memory_limit``
    – keeping the per-slice sort mostly in RAM instead of exhausting the
    spill directory. ``1`` means the single-pass merge suffices.

    Args:
        partition_bytes: Compressed parquet bytes of the partition (from
            the Delta log's add actions – no data scan).
        memory_limit: DuckDB memory limit string, typically
            ``Settings.duckdb_memory_limit``. Unparsable values (e.g. a
            percentage) fall back to `FALLBACK_MEMORY_LIMIT`.

    Returns:
        Slice count, at least ``1``.
    """
    try:
        budget = parse_byte_size(memory_limit)
    except ValueError:
        budget = parse_byte_size(FALLBACK_MEMORY_LIMIT)
    if partition_bytes <= 0:
        return 1
    return max(1, math.ceil(partition_bytes * MERGE_SPILL_FACTOR / budget))


def shard_target_file_size(shards: int) -> int:
    """Delta ``target_file_size`` for a re-shard write, bounding its memory.

    A re-shard scatters one ``(bucket, origin)`` group across every target
    shard, so ``write_deltalake`` holds one open partition writer per
    shard and each buffers up to ``target_file_size`` compressed bytes
    before it flushes. At the default `TARGET_SIZE`
    that peak is the file size *times the shard count* – gigabytes for the
    very datasets a re-shard is for. Dividing by the shard count keeps the
    peak at roughly one ``TARGET_SIZE`` regardless of how many shards the
    rows fan out into.

    Merge has no such problem – it writes one partition per call – and
    keeps the full ``TARGET_SIZE``. The smaller files a re-shard leaves
    behind are rewritten into one per partition by the next ``merge``, which
    the re-shard asks for anyway.

    Args:
        shards: Target shard count.

    Returns:
        Byte size, never below `SHARD_MIN_FILE_SIZE`.
    """
    return max(TARGET_SIZE // max(shards, 1), SHARD_MIN_FILE_SIZE)


def make_prune_by_shard(shards: int = 0) -> PruneFn:
    """Inject shard pruning into `SqlSource`"""

    def prune(q: Query) -> set[str]:
        values: set[str] = set()
        for f in q._leaves:
            if isinstance(f, IdLeaf):
                if f.comparator in ("eq", "in"):
                    for v in ensure_list(f.value):
                        values.add(entity_shard(v, shards))
        return values

    return prune
