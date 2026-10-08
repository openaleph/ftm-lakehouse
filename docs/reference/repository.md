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

A dataset's entities and statements: writes go through the journal, reads and maintenance to the parquet store.

```python
from ftmq.query import C, Query

from ftm_lakehouse import get_entities

entities = get_entities("my_dataset")
entities.add(entity, origin="import")
entities.writer(origin="import")
entities.flush()
entities.query(Query(C(origin="import")))
```

::: ftm_lakehouse.repository.EntityRepository
    options:
        heading_level: 3
        show_root_heading: true

## JobRepository

Job runs and their status, stored per job class – resolve the repository through the factory:

```python
from ftm_lakehouse.operation.crawl import CrawlJob
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

The read side of the exported document metadata (`documents.csv`, per origin scope).

::: ftm_lakehouse.repository.DocumentRepository
    options:
        heading_level: 3
        show_root_heading: true

## ArtifactsRepository

The export artifacts of one dataset – `statements.csv`, `entities.ftm.json`, `documents.csv` (one per origin scope), `parents.csv`, `statistics.json`, `index.json` – and the diff series of the diffable ones.

```python
from ftm_lakehouse.repository import get_artifacts

artifacts = get_artifacts("my_dataset")
artifacts.entities.exists()
artifacts.documents["crawl"].key
```

::: ftm_lakehouse.repository.ArtifactsRepository
    options:
        heading_level: 3
        show_root_heading: true

An artifact is a stateless declaration bound to a dataset; what is true only during an export – open writers, the diff window, counts – lives on its run, which `ExportSession` drives.

::: ftm_lakehouse.repository.artifacts.Artifact
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.repository.artifacts.DiffableArtifact
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.repository.artifacts.DocumentsArtifact
    options:
        heading_level: 3
        show_root_heading: true
