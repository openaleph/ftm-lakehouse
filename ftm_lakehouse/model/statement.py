"""Schema definitions for the sharded statement store.

Single source of truth for the parquet ``SHARDED_SCHEMA`` (pyarrow) and the
matching SQLAlchemy ``sharded_table()`` factory used to compose queries that
execute against DuckDB views over the parquet data.

The schema is: ``shard`` (entity-id hash bucket, hex-padded) prepended to
ftmq's ``ARROW_SCHEMA`` (all statement columns), with ``deleted_at`` appended
as a tombstone marker.
"""

from datetime import datetime
from typing import Generator, NamedTuple, TypeAlias

import pyarrow as pa
from followthemoney import Statement
from ftmq.store.lake import ARROW_SCHEMA
from nomenklatura import settings as nks
from sqlalchemy import Boolean, DateTime, TableClause, column, table

PA_TS = pa.timestamp("us", tz="UTC")
"""Timezone-aware microsecond timestamp type for metadata columns."""

# ftmq derives first_seen / last_seen from SQLAlchemy ``DateTime`` which yields
# tz-naive ``pa.timestamp("us")``. Override to tz-aware UTC so the entire
# timestamp surface (first_seen, last_seen, deleted_at) is consistent.
_TZ_AWARE_FIELDS = {"first_seen", "last_seen"}

SHARDED_SCHEMA = pa.schema(
    [
        pa.field("shard", pa.string()),
        *(
            pa.field(f.name, PA_TS) if f.name in _TZ_AWARE_FIELDS else f
            for f in ARROW_SCHEMA
        ),
        pa.field("deleted_at", PA_TS),
    ]
)
"""Parquet schema: ``shard`` + ftmq ``ARROW_SCHEMA`` (with tz-aware
timestamps) + ``deleted_at``."""

_PA_TO_SA = {
    pa.bool_(): Boolean,
    PA_TS: DateTime,
    pa.timestamp("us"): DateTime,
}


def _sharded_table(name: str) -> TableClause:
    """SQLAlchemy ``table()`` named ``name``, mirroring ``SHARDED_SCHEMA``.

    Use to compose queries that compile to DuckDB SQL via ``literal_binds``
    and execute against a registered view of the same name. Column types
    are derived from the pyarrow schema so the two stay in lockstep.
    """
    cols = []
    for field in SHARDED_SCHEMA:
        sa_type = _PA_TO_SA.get(field.type)
        cols.append(column(field.name, sa_type) if sa_type else column(field.name))
    return table(name, *cols)


# Default view name (``"statement"``) – this is the one the LakeStore
# connection registers as a *deduped* view in ftm_lakehouse, so read
# code targeting ``TABLE`` automatically sees one row per statement id
# with tombstones filtered.
TABLE = _sharded_table(nks.STATEMENT_TABLE)

# Raw view name (``"statement_raw"``) – registered alongside ``TABLE``
# on the same LakeStore connection and surfaces the underlying Delta
# rows unchanged. Targeted by paths that need tombstones and per-row
# physical layout visible: :func:`build_merge_query` (grace-period
# tombstone retention) and :meth:`get_changed_entity_ids` (diff
# consumers emit DEL ops).
TABLE_RAW = _sharded_table(f"{nks.STATEMENT_TABLE}_raw")


class StatementRow(NamedTuple):
    """In-memory statement row passed between the buffer and the parquet writer.

    Shared currency for both flush paths:
    - ``EntityBuffer.flush_buffer()`` (the bulk-write / direct-to-parquet path)
    - the journal flush path in ``EntityRepository.flush()``, which adapts
      each ``JournalRow`` into a ``StatementRow`` via ``unpack_statement``.
    """

    shard: str
    stmt: Statement
    deleted_at: datetime | None = None


StatementRows: TypeAlias = Generator[StatementRow, None, None]
