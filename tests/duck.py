"""DuckDB test helpers.

Bare DuckDB connections against a specific Delta table outside the
``LakeStore`` lifecycle – production code lets :class:`ftmq.store.lake.LakeStore`
manage connections and register the configured ``view_sqls`` instead.
"""

import duckdb
from deltalake import DeltaTable

from ftm_lakehouse.logic.parquet import duckdb_config, raw_view_sql
from ftm_lakehouse.model.statement import TABLE


def make_duckdb() -> duckdb.DuckDBPyConnection:
    """Stand-alone DuckDB connection with the lakehouse memory / spill config."""
    config: dict[str, str] = {
        "autoinstall_known_extensions": "true",
        "autoload_known_extensions": "true",
        **duckdb_config(),
    }
    con = duckdb.connect(":memory:", config=config)
    # Match production: render TIMESTAMPTZ in UTC, not the host timezone.
    con.execute("LOAD icu; SET TimeZone='UTC'")
    return con


def register_view(
    con: duckdb.DuckDBPyConnection,
    dt: DeltaTable,
    name: str = TABLE.name,
) -> None:
    """Register a raw ``delta_scan`` view named ``name`` on ``con``."""
    con.sql(f"CREATE OR REPLACE VIEW {name} AS {raw_view_sql(dt)}")
