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

from datetime import datetime

from deltalake import DeltaTable

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.statement import TABLE_RAW
from ftm_lakehouse.util import validate_origin

QUERY_IN_BATCH_SIZE = 5_000


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


def build_changed_sql(shard: str, bucket: str, since: datetime) -> str:
    """DuckDB SQL for the canonical live rows of entities changed since ``since``.

    :func:`_dedupe_sql` over the raw ``statement_raw`` view, scoped to one
    ``(shard, bucket)`` partition and semi-joined to the entities with a
    statement whose ``first_seen`` or ``deleted_at`` is newer than
    ``since`` – so the result matches what a post-merge read would return
    for those entities *without* requiring a merge first: supersession
    applied, tombstones shadowing their live rows and then filtered by
    the default ``deleted_at IS NULL`` predicate. A fully deleted entity
    therefore yields **zero** rows, which is what lets the diff exporter
    emit a ``DEL`` op on an un-merged store. The slice deliberately spans
    all origins of the partition – ``_dedupe_sql`` keys both branches on
    ``origin``, so per-origin rows stay isolated exactly as physical
    merge would leave them.

    Args:
        shard: Target shard value (hex-padded).
        bucket: Target bucket.
        since: Change watermark; compared against ``first_seen`` and
            ``deleted_at``.

    Returns:
        Executable DuckDB SQL, ordered by ``entity_id`` so each entity's
        rows stream contiguously into aggregation.
    """
    ts = f"TIMESTAMPTZ '{since.isoformat()}'"
    return _dedupe_sql(
        source=TABLE_RAW.name,
        where=(
            f"WHERE shard = '{shard}' AND bucket = '{bucket}' AND entity_id IN ("
            f"SELECT DISTINCT entity_id FROM {TABLE_RAW.name} "
            f"WHERE shard = '{shard}' AND bucket = '{bucket}' "
            f"AND (first_seen >= {ts} OR deleted_at >= {ts}))"
        ),
        order_by="ORDER BY entity_id",
    )


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
    origin = validate_origin(origin)
    return _dedupe_sql(
        source=TABLE_RAW.name,
        where=(
            f"WHERE shard = '{shard}' AND bucket = '{bucket}' "
            f"AND origin = '{origin}'"
        ),
        tombstone=(
            "(deleted_at IS NULL OR deleted_at > "
            f"TIMESTAMPTZ '{grace_cutoff.isoformat()}')"
        ),
        order_by="ORDER BY entity_id, fragment, prop, id, last_seen DESC",
    )
