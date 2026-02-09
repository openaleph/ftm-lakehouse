"""Statement serialization logic."""

from datetime import datetime, timezone

from followthemoney import Statement
from ftmq.store.base import DEFAULT_ORIGIN

NULL_BYTE = "\x00"


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


def pack_tombstone(stmt: Statement) -> str:
    """Pack only routing fields for a tombstone. Payload fields are empty."""
    parts = [
        stmt.id,
        stmt.entity_id,
        stmt.canonical_id or stmt.entity_id,
        "",  # prop (stripped)
        stmt.schema,
        "",  # value (stripped)
        stmt.dataset,
        "",  # lang
        "",  # original_value
        "0",  # external
        "",  # first_seen
        "",  # last_seen
        stmt.origin or DEFAULT_ORIGIN,
        "",  # prop_type
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


def unpack_tombstone_row(data: str) -> dict:
    """Unpack tombstone data directly into a dict matching ARROW_SCHEMA.

    Tombstones have stripped payload fields (empty prop, value, etc.) which
    would fail Statement validation. This function bypasses Statement and
    returns a dict suitable for write_deltalake.
    """
    from ftmq.store.lake import get_schema_bucket

    parts = data.split(NULL_BYTE)
    return {
        "id": parts[0],
        "entity_id": parts[1],
        "canonical_id": parts[2] or parts[1],
        "dataset": parts[6],
        "bucket": get_schema_bucket(parts[4]),
        "origin": parts[12] or DEFAULT_ORIGIN,
        "source": None,
        "schema": parts[4],
        "prop": parts[3] or None,
        "prop_type": parts[13] if len(parts) > 13 else None,
        "value": parts[5] or None,
        "original_value": parts[8] or None,
        "lang": parts[7] or None,
        "external": parts[9] == "1" if len(parts) > 9 else False,
        "first_seen": None,
        "last_seen": None,
    }
