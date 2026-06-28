# logic

The logic module contains pure, stateless transformation functions with no infrastructure dependencies. Functions here take inputs and produce outputs without side effects.

## Entity Aggregation

Aggregate a stream of statement dicts into FollowTheMoney entity dicts:

```python
from ftm_lakehouse.logic import aggregate_unsafe

for entity in aggregate_unsafe(statement_dicts, "my_dataset"):
    print(f"{entity['id']}: {entity['caption']}")
```

`aggregate_unsafe` assumes the input is pre-sorted by `canonical_id` – the parquet store guarantees this for its queries.

::: ftm_lakehouse.logic.aggregate_unsafe
    options:
        heading_level: 3
        show_root_heading: true

## Mapping Processing

Generate entities from FollowTheMoney mapping configurations:

```python
from ftm_lakehouse.logic import map_entities
from ftm_lakehouse.model.mapping import DatasetMapping

mapping = DatasetMapping(
    dataset="my_dataset",
    content_hash="abc123...",
    queries=[...]
)

for entity in map_entities(mapping, csv_path):
    print(f"{entity.schema.name}: {entity.caption}")
```

::: ftm_lakehouse.logic.map_entities
    options:
        heading_level: 3
        show_root_heading: true

## Parquet helpers

The DuckDB config, the `statement` / `statement_raw` view-SQL builders, and the merge-query builder used by `ParquetStore` via ftmq's `LakeStore`.

::: ftm_lakehouse.logic.parquet.duckdb_config
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.raw_view_sql
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.logic.parquet.dedupe_view_sql
    options:
        heading_level: 3
        show_root_heading: true

Both builders emit `delta_scan('<uri>')`, so a view defined from this SQL resolves the current Delta log on every query – defining it once per connection is enough; subsequent `write_deltalake` commits are picked up automatically. The deduped `statement` view windows over `(shard, bucket, id)` and hides tombstones, so every read (including stats) sees one live row per id; `statement_raw` exposes every physical row – tombstones and pre-merge duplicates included – for `merge` and `get_changed_entity_ids`.

::: ftm_lakehouse.logic.parquet.build_merge_query
    options:
        heading_level: 3
        show_root_heading: true

Returns a SQLAlchemy `Select` over the raw view that collapses one `(shard, bucket, origin)` partition; compile it to DuckDB SQL via `str(query.compile(compile_kwargs={"literal_binds": True}))`.

## Statement Serialization

Pack and unpack statements for compact storage in the journal `data` column:

```python
from ftm_lakehouse.logic import pack_statement, unpack_statement

packed = pack_statement(stmt)     # unit-separator delimited string
stmt   = unpack_statement(packed) # back to Statement
```

::: ftm_lakehouse.helpers.statements.pack_statement
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.helpers.statements.unpack_statement
    options:
        heading_level: 3
        show_root_heading: true
