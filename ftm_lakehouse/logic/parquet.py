"""Pure functions for Delta Lake parquet operations.

DuckDB view-SQL builders for ``LakeStore`` and the per-partition merge SQL.
The live ``statement`` view ([`live_view_sql`][live_view_sql]) is a plain
``WHERE deleted_at IS NULL`` scan – correctness assumes a store made
canonical by [`build_merge_sql`][build_merge_sql] (one row per id, supersession applied,
timestamps folded). ``statement_raw`` exposes every underlying Delta row for
code paths that need tombstones / pre-merge duplicates visible (``merge``,
``get_entity_ids`` over the raw source).

All dedupe / fragment-supersession / grace logic lives in one place –
`_dedupe_sql`, used only by [`build_merge_sql`][build_merge_sql]. See its docstring
for the two-branch fragment semantics.
"""

import math
from datetime import datetime

from banal import ensure_list
from deltalake import DeltaTable
from ftmq.query import Query
from ftmq.query.leaves import IdLeaf
from ftmq.query.sql import PruneFn
from ftmq.store.lake import TARGET_SIZE

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.statement import TABLE_RAW
from ftm_lakehouse.util import parse_byte_size, validate_origin

QUERY_IN_BATCH_SIZE = 5_000

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
trade its memory bound for a file-count explosion the follow-up ``compact``
has to clean up."""

FALLBACK_MEMORY_LIMIT = "8GB"
"""Slice budget when ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT`` is not a parseable
byte size (e.g. a DuckDB percentage limit) – mirrors the conservative
[`Settings`][ftm_lakehouse.core.settings.Settings] default."""


def duckdb_config() -> dict[str, str]:
    """LakeStore DuckDB config derived from lakehouse settings.

    Per-query memory is bounded by `Settings.duckdb_memory_limit`
    (env: ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``, default ``8GB``); queries
    exceeding the limit spill to `Settings.duckdb_temp_directory`
    (env: ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY``) when set, otherwise to
    the OS temp directory DuckDB picks by default. Extensions (notably
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


def _delta_scan_clause(dt: DeltaTable) -> str:
    """``delta_scan('<uri>')`` with the URI single-quote–escaped.

    DuckDB's ``delta_scan`` does not accept prepared parameters for its
    URI argument, so the URI is interpolated as a SQL string literal.
    Single quotes are doubled to prevent injection if a future code
    path lets a dataset name (and thus the URI) carry a quote – primary
    validation is in `validate_dataset_name`.
    """
    return f"delta_scan('{dt.table_uri.replace(chr(39), chr(39) * 2)}')"


def raw_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the ``statement_raw`` view.

    Surfaces every physical row in the Delta table, including
    tombstones and pre-merge duplicates. Used by [`build_merge_sql`][build_merge_sql]
    and raw-source queries (diff exports) – any path that needs the
    physical layout visible.
    """
    return f"SELECT * FROM {_delta_scan_clause(dt)}"


def _dedupe_sql(
    source: str,
    where: str = "",
    tombstone: str = "deleted_at IS NULL",
    order_by: str = "",
) -> str:
    """Two-branch dedupe skeleton for physical [`build_merge_sql`][build_merge_sql].

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
      scope of physical merge – load-bearing when the slice spans
      origins, e.g. `build_changed_sql`). Tombstones bump ``last_seen``
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
    row's date would both misdate it and hide it from
    `export_diff`, which detects change on ``first_seen``.

    The fragment branch folds by ``id`` too, not by its supersession group:
    the group spans *different* values, so folding across it would stamp a
    superseding value with the date of the row it replaced – both a false
    ``first_seen`` and, because ``first_seen`` is what
    `export_diff`
    detects change with, a silently undiffable update. Only the ``QUALIFY``
    windows below work at group scope, which is what supersession means.

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
        MIN(first_seen) OVER (
            PARTITION BY shard, bucket, origin, id, role
        ) AS first_seen
    )
    FROM base
    WHERE fragment = ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shard, bucket, origin, id, role
        ORDER BY last_seen DESC, deleted_at DESC NULLS LAST
    ) = 1
),
fragment_rows AS (
    SELECT * REPLACE (
        MIN(first_seen) OVER (
            PARTITION BY shard, bucket, origin, entity_id, prop, fragment, role, id
        ) AS first_seen
    )
    FROM base
    WHERE fragment != ''
    QUALIFY last_seen = MAX(last_seen) OVER (
        PARTITION BY shard, bucket, origin, entity_id, prop, fragment, role
    )
    AND ROW_NUMBER() OVER (
        PARTITION BY shard, bucket, origin, entity_id, prop, fragment, role, id
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

    On a store kept canonical by [`build_merge_sql`][build_merge_sql] (one row per
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
    [`raw_view_sql`][raw_view_sql] deliberately omits it so ``merge`` never materialises
    the duplicate column.

    Correctness holds only on an **optimized** store: between a write and
    the next `merge` this view can surface duplicate ids and rows
    whose delete has not been applied yet. Run ``optimize`` before
    querying – the dedupe / supersession / grace logic lives solely in
    [`build_merge_sql`][build_merge_sql].
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
        origin: Target origin tag – validated at the write boundary;
            single quotes are doubled here as defense in depth.
        grace_cutoff: Tombstones with ``deleted_at <= grace_cutoff`` are
            dropped. Typically ``now - LAKEHOUSE_GRACE_PERIOD_DAYS``.
        entity_id_range: Optional half-open ``[lo, hi)`` bound on
            ``entity_id`` (``None`` = unbounded on that side) scoping the
            merge to one range slice (`slice_ranges`). Every dedupe
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
        order_by="ORDER BY entity_id, fragment, role, prop, id, last_seen DESC",
    )


def shard_expr_sql(shards: int, column: str = "entity_id") -> str:
    """DuckDB expression computing the shard key of ``column``.

    The SQL twin of [`entity_shard`][ftm_lakehouse.core.conventions.path.entity_shard],
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
    width = path.shard_hex_width(shards)
    return (
        f"printf('%0{width}x', "
        f"(('0x' || substr(sha1({column}), 1, 8))::BIGINT) % {int(shards)})"
    )


def build_shard_sql(shard: str, bucket: str, origin: str, shards: int) -> str:
    """DuckDB SQL re-keying one partition's rows onto ``shards`` shards.

    ``SELECT *`` over the **raw** ``statement_raw`` view (tombstones and
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
        origin: Source origin tag – invariant under re-sharding; single
            quotes are doubled as defense in depth.
        shards: Target shard count.

    Returns:
        Executable DuckDB SQL.
    """
    origin = validate_origin(origin)
    return (
        f"SELECT * REPLACE ({shard_expr_sql(shards)} AS shard) "
        f"FROM {TABLE_RAW.name} "
        f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND origin = '{origin}'"
    )


def build_bounds_sample_sql(
    shard: str, bucket: str, origin: str, size: int = MERGE_SAMPLE_SIZE
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
    behind are what ``compact`` bin-packs on the next optimize, which the
    re-shard asks for anyway.

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
                        values.add(path.entity_shard(v, shards))
        return values

    return prune
