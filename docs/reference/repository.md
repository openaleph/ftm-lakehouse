# Layer 3: Repository

Domain-specific combinations of multiple stores. Each repository owns one domain concept.

## ArchiveRepository

Content-addressed file archive with metadata and extracted text storage.

```python
dataset.get_archive().put(uri)
dataset.get_archive().get(checksum)
dataset.get_archive().stream(file)
```

::: ftm_lakehouse.repository.ArchiveRepository
    options:
        heading_level: 3
        show_root_heading: true

## EntityRepository

Entity/statement operations combining JournalStore and ParquetStore.

```python
dataset.get_entities().add(entity, origin="import")
dataset.get_entities().writer(origin="import")
dataset.get_entities().flush()
dataset.get_entities().query(origin="import")
```

::: ftm_lakehouse.repository.EntityRepository
    options:
        heading_level: 3
        show_root_heading: true

## MappingRepository

Mapping configuration storage.

```python
dataset.get_mappings().put(mapping)
dataset.get_mappings().get(content_hash)
dataset.get_mappings().list()
```

::: ftm_lakehouse.repository.MappingRepository
    options:
        heading_level: 3
        show_root_heading: true

## JobRepository

Job tracking and status. Job runs are stored per job class – resolve the
repository through the factory:

```python
from ftm_lakehouse.repository.factories import get_jobs

jobs = get_jobs("my_dataset", CrawlJob)
jobs.put(job)
jobs.get(run_id)
```

::: ftm_lakehouse.repository.JobRepository
    options:
        heading_level: 3
        show_root_heading: true
