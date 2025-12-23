"""Mapping processing logic for CSV/tabular data transformations.

This module provides functions for generating FollowTheMoney entities
from tabular data using mapping configurations.
"""

from pathlib import Path

from anystore.io import logged_items
from anystore.logging import get_logger
from ftmq.types import Entities

from ftm_lakehouse.model.mapping import DatasetMapping, mapping_origin

log = get_logger(__name__)


def map_entities(mapping: DatasetMapping, csv_path: Path) -> Entities:
    """
    Generate entities from a mapping configuration and source file.

    Applies a FollowTheMoney mapping configuration to a CSV/tabular file
    and yields the resulting entities. Each entity is annotated with:

    - A `proof` property linking to the source file's content hash
    - An `origin` context identifying the mapping source

    This function is the core transformation logic used by DatasetMappings.process().
    It handles the iteration over mapping queries and record processing.

    Args:
        mapping: The mapping configuration containing query definitions
        csv_path: Local path to the source CSV/tabular file

    Yields:
        EntityProxy objects generated from the mapping

    Example:
        ```python
        from ftm_lakehouse.logic import map_entities
        from ftm_lakehouse.model.mapping import DatasetMapping

        mapping = DatasetMapping(
            dataset="my_dataset",
            content_hash="abc123...",
            queries=[...]  # FollowTheMoney mapping queries
        )

        for entity in map_entities(mapping, csv_path):
            print(f"{entity.schema.name}: {entity.caption}")
        ```

    See Also:

        - [FollowTheMoney Mappings](https://followthemoney.tech/docs/mappings/)
        - `operations.MappingOperation` for high-level mapping workflow
    """
    origin = mapping_origin(mapping.content_hash)
    for query in mapping.queries:
        mapper = query.make_mapping(csv_path.as_posix(), mapping.dataset)
        for record in logged_items(mapper.source.records, "Map", 1000, "Row", log):
            for entity in mapper.map(record).values():
                entity.add("proof", mapping.content_hash)
                entity.context["origin"] = [origin]
                yield entity
