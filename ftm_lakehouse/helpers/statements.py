"""Statement serialization logic."""

from datetime import datetime, timezone

from followthemoney import Statement
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import get_schema_bucket

from ftm_lakehouse.core.conventions.path import entity_shard

UNIT_SEP = "\x1f"


def make_order_key(stmt: Statement) -> str:
    """Build a journal order key from a Statement.

    Format: ``shard·bucket·origin·entity_id`` (delimited by UNIT_SEP).
    Lexicographic sort on this single column gives the same ordering as
    ``ORDER BY shard, bucket, origin, canonical_id``.
    """
    return UNIT_SEP.join(
        [
            entity_shard(stmt.entity_id),
            get_schema_bucket(stmt.schema),
            stmt.origin or DEFAULT_ORIGIN,
            stmt.entity_id,
        ]
    )


def parse_order_key(order_key: str) -> tuple[str, str, str, str]:
    """Split an order key back into (shard, bucket, origin, entity_id)."""
    shard, bucket, origin, entity_id = order_key.split(UNIT_SEP)
    return shard, bucket, origin, entity_id


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
