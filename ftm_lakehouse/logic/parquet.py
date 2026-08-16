"""Pure functions for Delta Lake parquet operations.

DuckDB view-SQL builders for ``LakeStore`` and the per-partition merge SQL.
The live ``statement`` view (:func:`live_view_sql`) is a plain
``WHERE deleted_at IS NULL`` scan – correctness assumes a store made
canonical by :func:`build_merge_sql` (one row per id, supersession applied,
timestamps folded). ``statement_raw`` exposes every underlying Delta row for
code paths that need tombstones / pre-merge duplicates visible (``merge``,
``get_changed_entity_ids``).

All dedupe / fragment-supersession / grace logic lives in one place –
:func:`_dedupe_sql`, used only by :func:`build_merge_sql`. See its docstring
for the two-branch fragment semantics.
"""

import math
from datetime import datetime

from banal import ensure_list
from deltalake import DeltaTable
from ftmq.query import Query
from ftmq.query.leaves import IdLeaf
from ftmq.query.sql import PruneFn

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.statement import TABLE_RAW
from ftm_lakehouse.util import parse_byte_size, validate_origin

QUERY_IN_BATCH_SIZE = 5_000

MERGE_SPILL_FACTOR = 32
"""Estimated peak DuckDB footprint of the merge pipeline per compressed
parquet byte – zstd/dictionary decompression blow-up (5–20x on statement
data) times the concurrent sort materialisations of :func:`_dedupe_sql`
(window groups + final ``ORDER BY``), padded for headroom. Used by
:func:`merge_slice_count` to bound each merge slice to the configured
DuckDB memory limit."""

MERGE_SAMPLE_SIZE = 10_000
"""Reservoir sample size for :func:`build_bounds_sample_sql`. Bounds the
slice-boundary resolution – boundary quality only affects load balance
across slices, never correctness, so a fixed sample is fine."""

FALLBACK_MEMORY_LIMIT = "8GB"
"""Slice budget when ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT`` is not a parseable
byte size (e.g. a DuckDB percentage limit) – mirrors the conservative
:class:`~ftm_lakehouse.core.settings.Settings` default."""


def duckdb_config() -> dict[str, str]:
    """LakeStore DuckDB config derived from lakehouse settings.

    Per-query memory is bounded by :attr:`Settings.duckdb_memory_limit`
    (env: ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``, default ``8GB``); queries
    exceeding the limit spill to :attr:`Settings.duckdb_temp_directory`
    (env: ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY``) when set, otherwise to
    the OS temp directory DuckDB picks by default. Extensions (notably
    ``delta``) are loaded from :attr:`Settings.duckdb_extension_directory`
    (env: ``LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY``) when set, otherwise
    from ``$HOME/.duckdb/extensions``. Passed to
    :class:`~ftmq.store.lake.LakeStore` via the ``duckdb_config`` kwarg.
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


def _delta_scan_clause(dt: DeltaTable) -> str:
    """``delta_scan('<uri>')`` with the URI single-quote–escaped.

    DuckDB's ``delta_scan`` does not accept prepared parameters for its
    URI argument, so the URI is interpolated as a SQL string literal.
    Single quotes are doubled to prevent injection if a future code
    path lets a dataset name (and thus the URI) carry a quote – primary
    validation is in :func:`ftm_lakehouse.util.validate_dataset_name`.
    """
    return f"delta_scan('{dt.table_uri.replace(chr(39), chr(39) * 2)}')"


def raw_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the ``statement_raw`` view.

    Surfaces every physical row in the Delta table, including
    tombstones and pre-merge duplicates. Used by :func:`build_merge_sql`
    and :meth:`get_changed_entity_ids` – any path that needs the
    physical layout visible.
    """
    return f"SELECT * FROM {_delta_scan_clause(dt)}"


def _dedupe_sql(
    source: str,
    where: str = "",
    tombstone: str = "deleted_at IS NULL",
    order_by: str = "",
) -> str:
    """Two-branch dedupe skeleton for physical :func:`build_merge_sql`.

    Rows route into two isolated branches on ``fragment`` (empty-string
    sentinel, applied *before* any window runs so the branches can never
    group with each other):

    - **non-fragment** (``fragment = ''``): at most one row per statement
      ``id`` per ``origin`` – ``QUALIFY ROW_NUMBER() OVER (... ORDER BY
      last_seen DESC, deleted_at DESC NULLS LAST) = 1`` picks the row
      with the latest ``last_seen``. Entity ids (and therefore statement
      ids) are uniquely placed in one ``(shard, bucket)`` by the model
      layer; ``origin`` in the key keeps the same id under two origins as
      two independent rows (matching the per-``(shard, bucket, origin)``
      scope of physical merge – load-bearing when the slice spans
      origins, e.g. :func:`build_changed_sql`). Tombstones bump
      ``last_seen = deleted_at`` at write time, so the tombstone wins
      ROW_NUMBER for a deleted statement – the ``deleted_at`` tiebreak
      keeps that deterministic even when delete and emission share a
      second – and the ``tombstone`` predicate decides whether it
      survives the final projection.
    - **fragment-bearing** (``fragment != ''``): supersession per
      ``(origin, entity_id, prop, fragment)`` group – ``QUALIFY
      last_seen = MAX(last_seen) OVER (...)`` admits the rows tied at
      the group's maximum ``last_seen`` (multi-valued props of one
      emission share their timestamp and survive together) and drops
      earlier emissions; among the tied rows the ANDed ``ROW_NUMBER``
      keeps one row per statement ``id``, so physically identical
      duplicates (a re-import of the same data) collapse and the merge
      is idempotent. ``origin`` in the group key keeps the same fragment
      under two origins as two independent supersession groups.

    Both branches fold ``first_seen`` to ``MIN(first_seen)`` over their
    group via ``SELECT * REPLACE``, so the surviving row carries its
    group's earliest observation (windows compute before ``QUALIFY``
    filters, so dropped duplicates still contribute their timestamps).

    Args:
        source: Relation to read from – a ``delta_scan('...')`` clause
            or a view name.
        where: Optional ``WHERE ...`` clause scoping ``source``.
        tombstone: Tombstone predicate applied after the branches union.
        order_by: Optional ``ORDER BY ...`` clause on the final output.

    Returns:
        Executable DuckDB SQL.
    """
    return f"""
WITH base AS (
    SELECT * FROM {source} {where}
),
nonfragment_rows AS (
    SELECT * REPLACE (
        MIN(first_seen) OVER (PARTITION BY shard, bucket, origin, id) AS first_seen
    )
    FROM base
    WHERE fragment = ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shard, bucket, origin, id
        ORDER BY last_seen DESC, deleted_at DESC NULLS LAST
    ) = 1
),
fragment_rows AS (
    SELECT * REPLACE (
        MIN(first_seen) OVER (
            PARTITION BY shard, bucket, origin, entity_id, prop, fragment
        ) AS first_seen
    )
    FROM base
    WHERE fragment != ''
    QUALIFY last_seen = MAX(last_seen) OVER (
        PARTITION BY shard, bucket, origin, entity_id, prop, fragment
    )
    AND ROW_NUMBER() OVER (
        PARTITION BY shard, bucket, origin, entity_id, prop, fragment, id
        ORDER BY last_seen DESC, deleted_at DESC NULLS LAST
    ) = 1
)
SELECT * FROM (
    SELECT * FROM nonfragment_rows
    UNION ALL
    SELECT * FROM fragment_rows
)
WHERE {tombstone}
{order_by}
""".strip()


def live_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the live ``statement`` view.

    On a store kept canonical by :func:`build_merge_sql` (one row per
    statement id, fragment supersession applied, ``first_seen`` /
    ``last_seen`` folded) the live rows are simply the non-tombstoned
    physical rows – so the view is a plain filtered scan, no window
    function. Predicate pushdown works natively: ``schema`` / ``prop`` /
    ``entity_id`` filters reach ``delta_scan``'s per-file statistics (a
    window would be a pushdown barrier for any non-partition column).

    ``canonical_id`` is not stored – this is a single-dataset store with no
    entity resolution, so it always equals ``entity_id`` – and is synthesised
    here as ``entity_id AS canonical_id`` so ftmq's query layer (which keys
    entity identity on ``canonical_id``) resolves against the view unchanged.
    :func:`raw_view_sql` deliberately omits it so ``merge`` never materialises
    the duplicate column.

    Correctness holds only on an **optimized** store: between a write and
    the next :meth:`merge` this view can surface duplicate ids and rows
    whose delete has not been applied yet. Run ``optimize`` before
    querying – the dedupe / supersession / grace logic lives solely in
    :func:`build_merge_sql`.
    """
    return (
        f"SELECT *, entity_id AS canonical_id "
        f"FROM {_delta_scan_clause(dt)} WHERE deleted_at IS NULL"
    )


def build_merge_sql(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
    entity_id_range: tuple[str | None, str | None] = (None, None),
) -> str:
    """DuckDB SQL that collapses one partition for physical merge.

    :func:`_dedupe_sql` over the **raw** ``statement_raw`` view (not the
    deduped ``statement``) because ``merge`` needs every row visible –
    including tombstones within the grace window, which must persist
    physically to keep shadowing their live rows – scoped to one
    ``(shard, bucket, origin)`` partition. Output is ordered by
    ``(entity_id, fragment, prop, id, last_seen DESC)`` – the file sort
    key – so the rewritten parquet file is ready for future merges
    without re-sort.

    Args:
        shard: Target shard value (hex-padded).
        bucket: Target bucket (``thing`` / ``interval`` / ``document`` /
            ``page`` / ``pages`` / ``mention``).
        origin: Target origin tag – validated at the write boundary;
            single quotes are doubled here as defense in depth.
        grace_cutoff: Tombstones with ``deleted_at <= grace_cutoff`` are
            dropped. Typically ``now - LAKEHOUSE_GRACE_PERIOD_DAYS``.
        entity_id_range: Optional half-open ``[lo, hi)`` bound on
            ``entity_id`` (``None`` = unbounded on that side) scoping the
            merge to one range slice (:func:`slice_ranges`). Every dedupe
            group is a function of a single entity – the non-fragment key
            ends in the statement ``id`` (owned by exactly one entity),
            the fragment key contains ``entity_id`` itself – so an
            ``entity_id`` predicate can never split a group.

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
        source=TABLE_RAW.name,
        where=where,
        tombstone=(
            "(deleted_at IS NULL OR deleted_at > "
            f"TIMESTAMPTZ '{grace_cutoff.isoformat()}')"
        ),
        order_by="ORDER BY entity_id, fragment, prop, id, last_seen DESC",
    )


def build_bounds_sample_sql(
    shard: str, bucket: str, origin: str, size: int = MERGE_SAMPLE_SIZE
) -> str:
    """DuckDB SQL reservoir-sampling ``entity_id`` values from one partition.

    Feeds :func:`slice_ranges` with boundary candidates for a range-sliced
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

    Returns:
        Executable DuckDB SQL yielding one ``entity_id`` column.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT entity_id FROM ("
        f"SELECT entity_id FROM {TABLE_RAW.name} "
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
            (:func:`build_bounds_sample_sql`).
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
    :data:`MERGE_SPILL_FACTOR` times the partition's compressed parquet
    size and slices so each slice's estimate fits within ``memory_limit``
    – keeping the per-slice sort mostly in RAM instead of exhausting the
    spill directory. ``1`` means the single-pass merge suffices.

    Args:
        partition_bytes: Compressed parquet bytes of the partition (from
            the Delta log's add actions – no data scan).
        memory_limit: DuckDB memory limit string, typically
            ``Settings.duckdb_memory_limit``. Unparsable values (e.g. a
            percentage) fall back to :data:`FALLBACK_MEMORY_LIMIT`.

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


def make_prune_by_shard(shards: int = 0) -> PruneFn:
    """Inject shard pruning into `SqlSource`"""

    def prune(q: Query) -> set[str]:
        values: set[str] = set()
        for f in q._leaves:
            if isinstance(f, IdLeaf):
                if f.comparator in ("eq", "in"):
                    for v in ensure_list(f.value):
                        values.add(path.entity_shard(v, shards))
        return values

    return prune
