"""Statement serialization logic."""

from datetime import datetime, timezone
from typing import Generator

from anystore.io.read import smart_stream_csv
from anystore.types import Uri
from followthemoney import Statement
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import LakeStatement

from ftm_lakehouse.exceptions import MalformedStatementError

UNIT_SEP = "\x1f"
"""Field separator used to pack a Statement into the journal ``data`` column."""

UNPACK_MIN_FIELDS = 12
"""Minimum field count :func:`unpack_statement` requires.

:func:`pack_statement` currently emits 13 fields (trailing ``prop_type``);
``unpack_statement`` only reads the first 12, so extra trailing fields
are tolerated for forward compatibility – but anything shorter is a
malformed row and rejected.
"""


def _to_iso(value: datetime | str | None) -> str:
    """Convert a datetime or string to ISO format string, ensuring UTC."""
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    return value


def pack_statement(stmt: Statement) -> str:
    """
    Pack a Statement into a unit-separator delimited string.

    Format: id, entity_id, prop, schema, value, dataset, lang,
            original_value, external, first_seen, last_seen, origin, prop_type

    ``canonical_id`` is not serialised – this store never resolves entities,
    so :func:`unpack_statement` lets FtM default it to ``entity_id``.
    """
    parts = [
        stmt.id or "",
        stmt.entity_id,
        stmt.prop,
        stmt.schema,
        stmt.value,
        stmt.dataset,
        stmt.lang or "",
        stmt.original_value or "",
        "1" if stmt.external else "0",
        _to_iso(stmt.first_seen),
        _to_iso(stmt.last_seen),
        stmt.origin or DEFAULT_ORIGIN,
        stmt.prop_type or "",
    ]
    return UNIT_SEP.join(parts)


def unpack_statement(data: str) -> Statement:
    """Unpack a unit-separator delimited string back into a Statement.

    Raises:
        MalformedStatementError: If ``data`` has fewer than
            :data:`UNPACK_MIN_FIELDS` separator-delimited fields. The
            journal flush loop catches this and logs+skips the row so
            one bad row can't abort an entire flush.
    """
    parts = data.split(UNIT_SEP)
    if len(parts) < UNPACK_MIN_FIELDS:
        raise MalformedStatementError(
            f"Packed statement has {len(parts)} fields; "
            f"expected at least {UNPACK_MIN_FIELDS}"
        )
    return Statement(
        id=parts[0] or None,
        entity_id=parts[1],  # required
        prop=parts[2],  # required
        schema=parts[3],  # required
        value=parts[4],  # required
        dataset=parts[5],  # required
        lang=parts[6] or None,
        original_value=parts[7] or None,
        external=parts[8] == "1",
        first_seen=parts[9] or None,
        last_seen=parts[10] or None,
        origin=parts[11] or None,
    )


def read_csv_statements(uri: Uri) -> Generator[LakeStatement, None, None]:
    """Stream a lakehouse ``statements.csv`` as :class:`LakeStatement` objects.

    followthemoney's ``read_csv_statements`` yields plain ``Statement`` objects
    and has no notion of the ``fragment`` supersession key, so the lakehouse
    needs its own reader. Rows are streamed as dicts via
    :func:`anystore.io.read.smart_stream_csv` and mapped straight to
    ``LakeStatement``; ``fragment`` is read from its column when present and
    falls back to the empty-string (non-fragment) sentinel otherwise.

    Args:
        uri: Location of the statements CSV.

    Yields:
        ``LakeStatement`` – ``canonical_id`` is left unset (FtM defaults it to
        ``entity_id``; this store does no resolution).
    """
    for row in smart_stream_csv(uri):
        yield LakeStatement(
            id=row.get("id") or None,
            entity_id=row["entity_id"],
            prop=row["prop"],
            schema=row["schema"],
            value=row.get("value") or "",
            dataset=row["dataset"],
            lang=row.get("lang") or None,
            original_value=row.get("original_value") or None,
            external=str(row.get("external", "")).strip().lower() in ("true", "1"),
            first_seen=row.get("first_seen") or None,
            last_seen=row.get("last_seen") or None,
            origin=row.get("origin") or None,
            fragment=row.get("fragment") or "",
        )
