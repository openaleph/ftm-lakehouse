"""Statement serialization logic."""

from datetime import datetime

from followthemoney import Statement
from ftmq.store.lake import DEFAULT_ORIGIN

NULL_BYTE = "\x00"


def _to_iso(value: datetime | str | None) -> str:
    """Convert a datetime or string to ISO format string."""
    if value is None:
        return datetime.now().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def pack_statement(stmt: Statement) -> str:
    """
    Pack a Statement into a null-byte joined string.

    Format: id, entity_id, canonical_id, prop, schema, value, dataset,
            lang, original_value, external, first_seen, last_seen, origin, prop_type
    """
    row = stmt.to_db_row()
    parts = [
        row["id"],  # required
        row["entity_id"],  # required
        row["canonical_id"],  # required
        row["prop"],  # required
        row["schema"],  # required
        row["value"],  # required
        row["dataset"],  # required
        row.get("lang") or "",
        row.get("original_value") or "",
        "1" if row.get("external") else "0",
        _to_iso(row.get("first_seen")),
        _to_iso(row.get("last_seen")),
        row.get("origin") or DEFAULT_ORIGIN,
        row.get("prop_type") or "",
    ]
    return NULL_BYTE.join(parts)


def unpack_statement(data: str) -> Statement:
    """
    Unpack a null-byte joined string back into a Statement.
    """
    parts = data.split(NULL_BYTE)
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
