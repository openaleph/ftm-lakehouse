# Layer 3: Repository

Domain-specific combinations of multiple stores. Each repository owns one domain concept.

## ArchiveRepository

Content-addressed file archive with metadata and extracted text storage.

```python
from ftm_lakehouse import get_archive

archive = get_archive("my_dataset")
archive.store(uri)
archive.get_file(checksum)
archive.stream(checksum)
```

::: ftm_lakehouse.repository.ArchiveRepository
    options:
        heading_level: 3
        show_root_heading: true

## EntityRepository

Entity/statement operations combining JournalStore and ParquetStore.

```python
from ftmq.query import M, Query

from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")
entities.add(entity, origin="import")
entities.writer(origin="import")
entities.flush()
entities.query(Query(M(origin="import")))
```

::: ftm_lakehouse.repository.EntityRepository
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

## DocumentRepository

Document metadata assembled from archived files and their entities.

::: ftm_lakehouse.repository.DocumentRepository
    options:
        heading_level: 3
        show_root_heading: true
