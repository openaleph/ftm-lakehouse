"""Pure functions for Delta Lake parquet operations.

DuckDB view-SQL builders for ``LakeStore`` and the SQLAlchemy ``Select``
that ``merge`` compiles per partition. Read-time dedupe lives in the
``statement`` view (window over ``(shard, bucket, id)``); ``statement_raw``
exposes the underlying Delta rows for code paths that need tombstones
or per-row physical layout visible (``merge``, ``get_changed_entity_ids``).
"""

from datetime import datetime

from deltalake import DeltaTable
from sqlalchemy import Select, func, or_, select

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
    tombstones and pre-merge duplicates. Used by :func:`build_merge_query`
    and :meth:`get_changed_entity_ids` – any path that needs the
    physical layout visible.
    """
    return f"SELECT * FROM {_delta_scan_clause(dt)}"


def dedupe_view_sql(dt: DeltaTable) -> str:
    """SELECT body for the deduped ``statement`` view.

    The view returns at most one row per statement ``id`` within each
    ``(shard, bucket)`` slice (entity ids – and therefore statement
    ids – are uniquely placed in one ``(shard, bucket)`` by the model
    layer, so the window's ``PARTITION BY shard, bucket, id`` matches a
    global window keyed by ``id``):

    - ``ROW_NUMBER() OVER (PARTITION BY shard, bucket, id ORDER BY
      last_seen DESC) = 1`` picks the row with the latest ``last_seen``
      per id. Tombstones bump ``last_seen = deleted_at`` at write time,
      so the tombstone wins ROW_NUMBER for a deleted statement; the
      outer ``deleted_at IS NULL`` then filters it out – a deleted
      entity is invisible to readers regardless of any surviving live
      row alongside it.
    - ``MIN(first_seen) OVER (PARTITION BY shard, bucket, id)``
      surfaces the earliest ``first_seen`` for each id under the
      ``first_seen`` column, so the dedupe matches what physical
      ``merge`` would produce.

    The column list is explicit so ``first_seen`` resolves to the
    windowed MIN and the helper columns (``rn``, ``first_seen_min``)
    aren't projected through to consumers.

    Partition predicates (``WHERE shard = ? AND bucket = ?``) push
    through this view to the parquet scan's File Filters because
    ``shard`` / ``bucket`` are in the window's ``PARTITION BY`` – the
    optimizer applies them before the window. That keeps each
    per-partition read bounded to one parquet file's worth of rows.
    """
    return f"""
SELECT
    id, entity_id, canonical_id, dataset, bucket, origin, source,
    schema, prop, prop_type, value, original_value, lang, external,
    first_seen_min AS first_seen, last_seen, deleted_at, shard
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY shard, bucket, id ORDER BY last_seen DESC
        ) AS rn,
        MIN(first_seen) OVER (PARTITION BY shard, bucket, id) AS first_seen_min
    FROM {_delta_scan_clause(dt)}
)
WHERE rn = 1 AND deleted_at IS NULL
""".strip()


def build_merge_query(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
) -> Select:
    """SQLAlchemy ``Select`` that collapses one partition for physical merge.

    Targets the **raw** ``statement_raw`` view (not the deduped
    ``statement``) because ``merge`` needs every row visible – including
    tombstones within the grace window – so it can rewrite the partition
    file in its physically-merged form. The returned query:

    - filters the raw view to one ``(shard, bucket, origin)`` partition;
    - computes ``MIN(first_seen) OVER (PARTITION BY id)`` so the surviving
      row carries the earliest ``first_seen`` for that statement id;
    - keeps the row with the latest ``last_seen`` per id via
      ``ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_seen DESC) = 1``;
    - drops tombstones whose ``deleted_at`` is older than ``grace_cutoff``;
    - orders by ``(entity_id, id, last_seen DESC)`` so the rewritten parquet
      file is ready for future merges without re-sort.

    Compile to executable DuckDB SQL with
    ``str(query.compile(compile_kwargs={"literal_binds": True}))``.

    Args:
        shard: Target shard value (hex-padded).
        bucket: Target bucket (``thing`` / ``interval`` / ``document`` /
            ``page`` / ``pages`` / ``mention``).
        origin: Target origin tag.
        grace_cutoff: Tombstones with ``deleted_at <= grace_cutoff`` are
            dropped. Typically ``now - LAKEHOUSE_GRACE_PERIOD_DAYS``.

    Returns:
        A SQLAlchemy :class:`~sqlalchemy.sql.expression.Select` that
        compiles to DuckDB SQL.
    """
    inner_cols = [c for c in TABLE_RAW.columns if c.name != "first_seen"]
    inner = (
        select(
            *inner_cols,
            func.min(TABLE_RAW.c.first_seen)
            .over(partition_by=TABLE_RAW.c.id)
            .label("first_seen"),
            func.row_number()
            .over(partition_by=TABLE_RAW.c.id, order_by=TABLE_RAW.c.last_seen.desc())
            .label("rn"),
        )
        .where(
            TABLE_RAW.c.shard == shard,
            TABLE_RAW.c.bucket == bucket,
            TABLE_RAW.c.origin == origin,
        )
        .subquery("merge_src")
    )

    return (
        select(*[c for c in inner.c if c.name != "rn"])
        .where(
            inner.c.rn == 1,
            or_(
                inner.c.deleted_at.is_(None),
                inner.c.deleted_at > grace_cutoff,
            ),
        )
        .order_by(inner.c.entity_id, inner.c.id, inner.c.last_seen.desc())
    )
