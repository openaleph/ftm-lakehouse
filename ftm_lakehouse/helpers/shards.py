"""Shard key derivation for the statement store's ``shard`` partition.

A shard is a pure function of the entity id and the dataset's configured shard
count, which is what lets every reader and writer agree on where an entity's
statements live without coordinating. `build_shard_sql` in
``logic/parquet.py`` is the SQL twin of [`entity_shard`][entity_shard] – the
two must stay in step, and a test pins that they do.
"""

from banal import hash_data


def shard_hex_width(shards: int) -> int:
    """Hex width required to represent `shards-1` (zero-padded).

    Examples: 1→1, 8→1, 16→1, 32→2, 256→2, 4096→3.
    """
    if shards <= 1:
        return 1
    return max(1, ((shards - 1).bit_length() + 3) // 4)


def entity_shard(entity_id: str, shards: int) -> str:
    """Hex shard key for an entity id under a uniform shard count.

    Uses the first 8 hex chars of the entity_id hash, taken mod ``shards``,
    then zero-padded to `shard_hex_width`.
    """
    if shards <= 1:
        return "0"
    bucket = int(hash_data(entity_id)[:8], 16) % shards
    return f"{bucket:0{shard_hex_width(shards)}x}"
