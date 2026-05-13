"""Pure functions for Delta Lake parquet operations.

DuckDB helpers to register a Delta table as a SQL view and to compose merge
queries via SQLAlchemy. Shard partitioning bounds the size of each query's
input, so disk-spill plumbing (custom ``temp_directory``, ``memory_limit``)
is not needed and relies on DuckDB defaults (see how this goes...)
"""

from datetime import datetime

import duckdb
from deltalake import DeltaTable
from sqlalchemy import Select, func, or_, select

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model.statement import TABLE

QUERY_IN_BATCH_SIZE = 5_000


def make_duckdb() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with the configured memory ceiling and
    spill directory.

    Per-query memory is bounded by :attr:`Settings.duckdb_memory_limit`
    (env: ``LAKEHOUSE_DUCKDB_MEMORY_LIMIT``, default ``4GB``); queries
    exceeding the limit spill to
    :attr:`Settings.duckdb_temp_directory`
    (env: ``LAKEHOUSE_DUCKDB_TEMP_DIRECTORY``) when set, otherwise to
    the OS temp directory DuckDB picks by default.
    """
    settings = Settings()
    config: dict[str, str] = {"memory_limit": settings.duckdb_memory_limit}
    if settings.duckdb_temp_directory:
        config["temp_directory"] = settings.duckdb_temp_directory
    return duckdb.connect(":memory:", config=config)


def register_view(
    con: duckdb.DuckDBPyConnection,
    dt: DeltaTable,
    name: str = TABLE.name,
) -> None:
    """Register a DeltaTable as a DuckDB view via ``delta_scan``.

    The Delta extension exposes partition values as columns and uses Delta's
    column statistics for file skipping on filtered queries, so per-partition
    queries (``WHERE shard = ? AND bucket = ? AND origin = ?``) prune to one
    partition's files automatically.

    DuckDB's ``delta_scan`` does not accept prepared parameters for its URI
    argument, so the URI is interpolated as a SQL string literal. Single
    quotes are doubled to prevent injection if a future code path lets a
    dataset name (and thus the URI) carry a quote – primary validation is
    in :func:`ftm_lakehouse.util.validate_dataset_name`.
    """
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    table_uri = dt.table_uri.replace("'", "''")
    con.sql(
        f"CREATE OR REPLACE VIEW {name} AS " f"SELECT * FROM delta_scan('{table_uri}')"
    )


def build_merge_query(
    shard: str,
    bucket: str,
    origin: str,
    grace_cutoff: datetime,
) -> Select:
    """SQLAlchemy ``Select`` that collapses one partition.

    The returned query:

    - filters the source view to one ``(shard, bucket, origin)`` partition;
    - computes ``MIN(first_seen) OVER (PARTITION BY id)`` so the surviving
      row carries the earliest ``first_seen`` for that statement id;
    - keeps the row with the latest ``last_seen`` per id via
      ``ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_seen DESC) = 1``;
    - drops tombstones whose ``deleted_at`` is older than ``grace_cutoff``;
    - orders by ``(entity_id, id, last_seen DESC)`` so the rewritten parquet
      file is ready for future merges without re-sort.

    Consumers can compose further filters via ``.where(...)`` on the
    returned Select (e.g.
    ``query.where(query.selected_columns.entity_id == entity_id)`` for a
    single-entity merge). Compile to executable DuckDB SQL with
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
    inner_cols = [c for c in TABLE.columns if c.name != "first_seen"]
    inner = (
        select(
            *inner_cols,
            func.min(TABLE.c.first_seen)
            .over(partition_by=TABLE.c.id)
            .label("first_seen"),
            func.row_number()
            .over(partition_by=TABLE.c.id, order_by=TABLE.c.last_seen.desc())
            .label("rn"),
        )
        .where(
            TABLE.c.shard == shard,
            TABLE.c.bucket == bucket,
            TABLE.c.origin == origin,
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
