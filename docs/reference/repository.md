# Layer 3: Repository

Domain-specific combinations of multiple stores. Each repository owns one domain concept.

## ArchiveRepository

Content-addressed file archive with metadata and extracted text storage.

```python
dataset.archive.put(uri)
dataset.archive.get(checksum)
dataset.archive.stream(file)
```

::: ftm_lakehouse.repository.ArchiveRepository
    options:
        heading_level: 3
        show_root_heading: true

## EntityRepository

Entity/statement operations combining JournalStore and ParquetStore.

```python
dataset.entities.add(entity, origin="import")
dataset.entities.bulk(origin="import")
dataset.entities.flush()
dataset.entities.query(origin="import")
```

::: ftm_lakehouse.repository.EntityRepository
    options:
        heading_level: 3
        show_root_heading: true

## MappingRepository

Mapping configuration storage.

```python
dataset.mappings.put(mapping)
dataset.mappings.get(content_hash)
dataset.mappings.list()
```

::: ftm_lakehouse.repository.MappingRepository
    options:
        heading_level: 3
        show_root_heading: true

## JobRepository

Job tracking and status.

```python
dataset.jobs.put(job)
dataset.jobs.get(run_id)
```

::: ftm_lakehouse.repository.JobRepository
    options:
        heading_level: 3
        show_root_heading: true
