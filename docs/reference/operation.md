# Layer 4: Operation

Multi-step workflow operations that coordinate across repositories.

## Base Classes

::: ftm_lakehouse.operation.base.DatasetJobOperation
    options:
        heading_level: 3
        show_root_heading: true

## CrawlOperation

Batch file ingestion from a source location.

::: ftm_lakehouse.operation.crawl.CrawlJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.CrawlOperation
    options:
        heading_level: 3
        show_root_heading: true

## ExportOperation

One operation for all exports, dispatched by `ExportKind`: `statements` (`exports/statements.csv`), `entities` (`entities.ftm.json`), `documents` (`exports/documents.csv`), `statistics` (`exports/statistics.json`), `index` (`index.json`).

::: ftm_lakehouse.operation.export.ExportKind
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.export.ExportJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.ExportOperation
    options:
        heading_level: 3
        show_root_heading: true

## MappingOperation

Process CSV-to-entity mapping configurations.

::: ftm_lakehouse.operation.mapping.MappingJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.MappingOperation
    options:
        heading_level: 3
        show_root_heading: true

## OptimizeOperation

Optimize the parquet statement store in one pass: merge (per-partition rewrite that collapses duplicates, folds `first_seen` to the min, drops tombstones older than the grace cutoff per `LAKEHOUSE_GRACE_PERIOD_DAYS`), compact (bin-pack small files) and vacuum (delete obsolete files). Each step acquires the dataset-wide write fence (`.LOCK`).

::: ftm_lakehouse.operation.maintenance.OptimizeJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.OptimizeOperation
    options:
        heading_level: 3
        show_root_heading: true

## MakeOperation

Full workflow: flush journal + all exports.

::: ftm_lakehouse.operation.make.MakeJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.MakeOperation
    options:
        heading_level: 3
        show_root_heading: true

## DownloadArchiveOperation

Export archive files to their original paths.

::: ftm_lakehouse.operation.download.DownloadArchiveJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.DownloadArchiveOperation
    options:
        heading_level: 3
        show_root_heading: true
