"""Pure functions for Delta Lake parquet operations.

DuckDB view-SQL builders for ``LakeStore`` and the per-partition merge
SQL. Read-time dedupe lives in the ``statement`` view; ``statement_raw``
exposes the underlying Delta rows for code paths that need tombstones or
per-row physical layout visible (``merge``, ``get_changed_entity_ids``).

The ``statement`` view and ``merge`` compile from one shared skeleton
(:func:`_dedupe_sql`), so read-time dedupe matches what a physical merge
would produce by construction. See its docstring for the two-branch
fragment semantics.
"""

from datetime import datetime

from deltalake import DeltaTable

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.statement import TABLE_RAW

QUERY_IN_BATCH_SIZE = 5_000


def duckdb_config() -> dict[str, str]:
    """LakeStore DuckDB config derived from lakehouse settings.

    Per-query memory is bounded by :attr:`Settings.duckdb_memory_limit`
    (env: ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``, default ``4GB``); queries
    exceeding the limit spill to :attr:`Settings.duckdb_temp_directory`
    (env: ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY``) when set, otherwise to
    the OS temp directory DuckDB picks by default. Passed to
    :class:`~ftmq.store.lake.LakeStore` via the ``duckdb_config`` kwarg.
    """
    settings = Settings()
    config: dict[str, str] = {"memory_limit": settings.duckdb_memory_limit}
    if settings.duckdb_temp_directory:
        config["temp_directory"] = settings.duckdb_temp_directory
    return config


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
    """Shared two-branch dedupe skeleton for the ``statement`` view and merge.

    Rows route into two isolated branches on ``fragment`` (empty-string
    sentinel, applied *before* any window runs so the branches can never
    group with each other):

    - **non-fragment** (``fragment = ''``): at most one row per statement
      ``id`` – ``QUALIFY ROW_NUMBER() OVER (... ORDER BY last_seen DESC)
      = 1`` picks the row with the latest ``last_seen``. Entity ids (and
      therefore statement ids) are uniquely placed in one ``(shard,
      bucket)`` by the model layer, so partitioning by ``(shard, bucket,
      id)`` matches a global window keyed by ``id``. Tombstones bump
      ``last_seen = deleted_at`` at write time, so the tombstone wins
      ROW_NUMBER for a deleted statement and the ``tombstone`` predicate
      decides whether it survives the final projection.
    - **fragment-bearing** (``fragment != ''``): supersession per
      ``(origin, entity_id, prop, fragment)`` group – ``QUALIFY
      last_seen = MAX(last_seen) OVER (...)`` admits *every* row tied at
      the group's maximum ``last_seen`` (multi-valued props of one
      emission share their timestamp and survive together) and drops
      earlier emissions. ``origin`` in the group key keeps the same
      fragment under two origins as two independent supersession groups,
      matching the per-``(shard, bucket, origin)`` scope of physical
      merge.

    Both branches fold ``first_seen`` to ``MIN(first_seen)`` over their
    group via ``SELECT * REPLACE``, so the surviving row carries its
    group's earliest observation and read-time results match post-merge
    results exactly.

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
        MIN(first_seen) OVER (PARTITION BY shard, bucket, id) AS first_seen
    )
    FROM base
    WHERE fragment = ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shard, bucket, id ORDER BY last_seen DESC
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
)
SELECT * FROM (
    SELECT * FROM nonfragment_rows
    UNION ALL
    SELECT * FROM fragment_rows
)
WHERE {tombstone}
{order_by}
""".strip()


def dedupe_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the deduped ``statement`` view.

    :func:`_dedupe_sql` over the live Delta scan with tombstones hidden
    (``deleted_at IS NULL``) – a deleted entity is invisible to readers
    regardless of any surviving live row alongside its tombstone.

    Partition predicates (``WHERE shard = ? AND bucket = ?``) push
    through this view to the parquet scan's File Filters because
    ``shard`` / ``bucket`` are in both windows' ``PARTITION BY`` – the
    optimizer applies them before the window. That keeps each
    per-partition read bounded to one parquet file's worth of rows.
    """
    return _dedupe_sql(source=_delta_scan_clause(dt))


def build_merge_sql(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
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

    Returns:
        Executable DuckDB SQL.
    """
    safe_origin = origin.replace("'", "''")
    return _dedupe_sql(
        source=TABLE_RAW.name,
        where=(
            f"WHERE shard = '{shard}' AND bucket = '{bucket}' "
            f"AND origin = '{safe_origin}'"
        ),
        tombstone=(
            "(deleted_at IS NULL OR deleted_at > "
            f"TIMESTAMPTZ '{grace_cutoff.isoformat()}')"
        ),
        order_by="ORDER BY entity_id, fragment, prop, id, last_seen DESC",
    )
