# Layer 4: Operation

Multi-step workflow operations that coordinate across repositories.

## Base Classes

::: ftm_lakehouse.operation.base.DatasetJobOperation
    options:
        heading_level: 3
        show_root_heading: true

## Export Operations

### ExportStatementsOperation

Export parquet store to `exports/statements.csv`.

::: ftm_lakehouse.operation.ExportStatementsOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportEntitiesOperation

Export parquet store to `entities.ftm.json`.

::: ftm_lakehouse.operation.ExportEntitiesOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportStatisticsOperation

Export statistics to `exports/statistics.json`.

::: ftm_lakehouse.operation.ExportStatisticsOperation
    options:
        heading_level: 4
        show_root_heading: true

### ExportIndexOperation

Export `index.json` with optional resources.

::: ftm_lakehouse.operation.ExportIndexOperation
    options:
        heading_level: 4
        show_root_heading: true

## CrawlOperation

Batch file ingestion from a source location.

::: ftm_lakehouse.operation.CrawlOperation
    options:
        heading_level: 3
        show_root_heading: true

## MappingOperation

Process CSV-to-entity mapping configurations.

::: ftm_lakehouse.operation.MappingOperation
    options:
        heading_level: 3
        show_root_heading: true

## OptimizeOperation

Compact Delta Lake parquet files.

::: ftm_lakehouse.operation.OptimizeOperation
    options:
        heading_level: 3
        show_root_heading: true
