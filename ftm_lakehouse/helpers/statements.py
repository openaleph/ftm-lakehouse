"""Statement serialization logic."""

from datetime import datetime, timezone

from followthemoney import Statement
from ftmq.store.base import DEFAULT_ORIGIN

UNIT_SEP = "\x1f"
"""Field separator used to pack a Statement into the journal ``data`` column."""


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

    Format: id, entity_id, canonical_id, prop, schema, value, dataset,
            lang, original_value, external, first_seen, last_seen, origin, prop_type
    """
    parts = [
        stmt.id or "",
        stmt.entity_id,
        stmt.canonical_id or stmt.entity_id,
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
    """
    Unpack a unit-separator delimited string back into a Statement.
    """
    parts = data.split(UNIT_SEP)
    return Statement(
        id=parts[0] or None,
        entity_id=parts[1],  # required
        canonical_id=parts[2] or None,
        prop=parts[3],  # required
        schema=parts[4],  # required
        value=parts[5],  # required
        dataset=parts[6],  # required
        lang=parts[7] or None,
        original_value=parts[8] or None,
        external=parts[9] == "1",
        first_seen=parts[10] or None,
        last_seen=parts[11] or None,
        origin=parts[12] or None,
    )
