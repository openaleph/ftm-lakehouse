"""Business logic / domain services layer.

This module contains the core business logic separated from storage concerns.
It provides reusable functions for entity processing, mapping transformations,
and document crawling that can be used by client applications.

Modules:
    entities: Statement aggregation and entity assembly
    mappings: FollowTheMoney mapping processing for CSV transformations
    crawl: Document crawling from local and remote sources

Example:
    ```python
    from ftm_lakehouse.logic import aggregate_statements, map_entities, crawl

    # Aggregate statements into entities
    for entity in aggregate_statements(statements, "my_dataset"):
        process(entity)

    # Generate entities from mapping
    for entity in map_entities(mapping, archive, csv_path):
        store(entity)

    # Crawl documents
    result = crawl("/path/to/docs", dataset, glob="*.pdf")
    ```
"""

from ftm_lakehouse.logic.crawl import CRAWL_ORIGIN, CrawlJob, CrawlWorker, crawl
from ftm_lakehouse.logic.entities import aggregate_statements
from ftm_lakehouse.logic.mappings import map_entities

__all__ = [
    "CRAWL_ORIGIN",
    "CrawlJob",
    "CrawlWorker",
    "aggregate_statements",
    "crawl",
    "map_entities",
]
