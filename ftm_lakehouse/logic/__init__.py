"""Pure business logic layer.

This module contains stateless transformation functions with no infrastructure
dependencies. Functions here take inputs and produce outputs without side effects.

Modules:
    entities: Statement aggregation and entity assembly
    mappings: FollowTheMoney mapping processing for CSV transformations
    statements: Statement serialization (pack/unpack)

Example:
    ```python
    from ftm_lakehouse.logic import aggregate_statements, map_entities

    # Aggregate statements into entities
    for entity in aggregate_statements(statements, "my_dataset"):
        process(entity)

    # Generate entities from mapping
    for entity in map_entities(mapping, archive, csv_path):
        store(entity)
    ```
"""

from ftm_lakehouse.helpers.statements import pack_statement, unpack_statement
from ftm_lakehouse.logic.entities import aggregate_statements
from ftm_lakehouse.logic.mappings import map_entities

__all__ = [
    "aggregate_statements",
    "map_entities",
    "pack_statement",
    "unpack_statement",
]
