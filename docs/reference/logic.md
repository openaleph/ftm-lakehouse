# logic

The logic module contains core business logic for entity processing, mapping transformations, and document crawling. These functions are designed to be used by client applications building on top of the data lakehouse.

## Entity Aggregation

Aggregate statement streams into FollowTheMoney entities:

```python
from ftm_lakehouse.logic import aggregate_statements
from followthemoney.statement.serialize import read_csv_statements

with open("statements.csv") as f:
    statements = read_csv_statements(f)
    for entity in aggregate_statements(statements, "my_dataset"):
        print(f"{entity.id}: {entity.caption}")
```

::: ftm_lakehouse.logic.aggregate_statements
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

## Document Crawling

Crawl local or remote document collections into a dataset:

```python
from ftm_lakehouse import get_dataset
from ftm_lakehouse.logic import crawl

dataset = get_dataset("my_dataset")

# Crawl local directory
result = crawl("/path/to/documents", dataset, glob="*.pdf")
print(f"Crawled {result.done} files")

# Crawl S3 bucket
result = crawl(
    "s3://my-bucket/docs",
    dataset,
    prefix="2024/",
    exclude_glob="*.tmp"
)
```

::: ftm_lakehouse.logic.crawl
    options:
        heading_level: 3
        show_root_heading: true

### CrawlJob

::: ftm_lakehouse.logic.CrawlJob
    options:
        heading_level: 4
        show_root_heading: true
        members:
            - uri
            - skip_existing
            - cache_key_uri
            - prefix
            - exclude_prefix
            - glob
            - exclude_glob

### CrawlWorker

::: ftm_lakehouse.logic.CrawlWorker
    options:
        heading_level: 4
        show_root_heading: true
        members:
            - get_tasks
            - handle_task
            - run
