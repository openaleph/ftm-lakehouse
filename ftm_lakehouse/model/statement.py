"""Schema definitions for the sharded statement store.

Single source of truth for the parquet ``SHARDED_SCHEMA`` (pyarrow) and the
matching SQLAlchemy ``sharded_table()`` factory used to compose queries that
execute against DuckDB views over the parquet data.

The schema is: ``shard`` (entity-id hash bucket, hex-padded) prepended to
ftmq's ``ARROW_SCHEMA`` (all statement columns, including ``fragment`` – the
supersession group key, empty-string sentinel for non-fragment rows) minus
``canonical_id``, with ``deleted_at`` (tombstone marker) appended.

``canonical_id`` is dropped from physical storage: this is a single-dataset
store with no entity resolution, so ``canonical_id`` always equals
``entity_id``. It is re-derived as ``entity_id AS canonical_id`` in the live
``statement`` view (:func:`ftm_lakehouse.logic.parquet.live_view_sql`) so
ftmq's query layer (which keys entity identity on ``canonical_id``) keeps
working unchanged.
"""

from datetime import datetime
from typing import Any, Generator, NamedTuple, TypeAlias

import pyarrow as pa
from ftmq.store.lake import ARROW_SCHEMA, LakeStatement
from nomenklatura import settings as nks
from sqlalchemy import Boolean, DateTime, Select, TableClause, column, select, table

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
            if f.name != "canonical_id"
        ),
        pa.field("deleted_at", PA_TS),
    ]
)
"""Parquet schema: ``shard`` + ftmq ``ARROW_SCHEMA`` (with tz-aware
timestamps, minus ``canonical_id``) + ``deleted_at``.

``fragment`` (part of ``ARROW_SCHEMA``) uses the empty string – never
NULL – as the "no fragment" sentinel; :class:`ftmq.store.lake.LakeStatement`
guarantees it on every write path."""

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
# physical layout visible: :func:`build_merge_sql` (grace-period
# tombstone retention) and raw-source ``get_entity_ids`` queries (diff
# consumers emit DEL ops).
TABLE_RAW = _sharded_table(f"{nks.STATEMENT_TABLE}_raw")


STATEMENT_CSV_COLUMNS = [
    "id",
    "entity_id",
    "canonical_id",
    "prop",
    "prop_type",
    "schema",
    "value",
    "original_value",
    "dataset",
    "origin",
    "lang",
    "external",
    "first_seen",
    "last_seen",
    "fragment",
]
"""Columns of the exported ``statements.csv``: the followthemoney standard set
plus the lakehouse ``fragment`` supersession key, so ``statements import``
round-trips it (followthemoney's ``read_csv_statements`` has no notion of
``fragment`` – :func:`ftm_lakehouse.helpers.statements.read_csv_statements`
reads it back). ``canonical_id`` is the ``entity_id`` alias, kept for FtM
interop."""

_STATEMENT_CSV_TABLE = table(
    nks.STATEMENT_TABLE, *(column(c) for c in STATEMENT_CSV_COLUMNS)
)


def statement_csv_select() -> Select[Any]:
    """SELECT of :data:`STATEMENT_CSV_COLUMNS` from the live ``statement`` view,
    ordered by ``entity_id`` (so an entity's rows stay contiguous for
    per-partition streaming exports)."""
    return select(_STATEMENT_CSV_TABLE).order_by(_STATEMENT_CSV_TABLE.c.entity_id)


class StatementRow(NamedTuple):
    """In-memory statement row passed between the buffer and the parquet writer.

    Shared currency for both flush paths:
    - ``EntityBuffer.flush_buffer()`` (the bulk-write / direct-to-parquet path)
    - the journal flush path in ``EntityRepository.flush()``, which adapts
      each ``JournalRow`` into a ``StatementRow`` via ``unpack_journal_row``.

    ``stmt`` is a :class:`ftmq.store.lake.LakeStatement` – it carries the
    supersession group key ``fragment`` itself (empty string means
    non-fragment, content-addressed dedup by ``id``).
    """

    shard: str
    stmt: LakeStatement
    deleted_at: datetime | None = None


StatementRows: TypeAlias = Generator[StatementRow, None, None]
