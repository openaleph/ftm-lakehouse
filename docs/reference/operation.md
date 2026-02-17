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

## Export Operations

### ExportStatementsOperation

Export parquet store to `exports/statements.csv`.

::: ftm_lakehouse.operation.export.ExportStatementsJob
    options:
        heading_level: 4
        show_root_heading: true

::: ftm_lakehouse.operation.ExportStatementsOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportEntitiesOperation

Export parquet store to `entities.ftm.json`.

::: ftm_lakehouse.operation.export.ExportEntitiesJob
    options:
        heading_level: 4
        show_root_heading: true

::: ftm_lakehouse.operation.ExportEntitiesOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportStatisticsOperation

Export statistics to `exports/statistics.json`.

::: ftm_lakehouse.operation.export.ExportStatisticsJob
    options:
        heading_level: 4
        show_root_heading: true

::: ftm_lakehouse.operation.ExportStatisticsOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportDocumentsOperation

Export document metadata to `exports/documents.csv`.

::: ftm_lakehouse.operation.export.ExportDocumentsJob
    options:
        heading_level: 4
        show_root_heading: true

::: ftm_lakehouse.operation.ExportDocumentsOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportIndexOperation

Export `index.json` with optional resources.

::: ftm_lakehouse.operation.export.ExportIndexJob
    options:
        heading_level: 4
        show_root_heading: true

::: ftm_lakehouse.operation.ExportIndexOperation
    options:
        heading_level: 4
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

Compact Delta Lake parquet files and optionally apply translog to main table.

::: ftm_lakehouse.operation.optimize.OptimizeJob
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

## RecreateOperation

Repair corrupted datasets from exported files.

::: ftm_lakehouse.operation.recreate.RecreateJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.recreate.RecreateOperation
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
