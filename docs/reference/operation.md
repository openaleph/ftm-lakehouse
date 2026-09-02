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

One operation for all exports, dispatched by `ExportKind`: `statements` (`exports/statements.csv`), `entities` (`entities.ftm.json`), `documents` (`exports/documents.csv`, plus `exports/documents.crawl.csv` scoped to crawled files – each with its own diff series), `statistics` (`exports/statistics.json`), `index` (`index.json`).

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

## OptimizeOperation

Optimize the parquet statement store in one pass: merge (per-partition rewrite that collapses duplicates, folds `first_seen` to the min, `last_seen` to the max, drops tombstones older than the grace cutoff per `LAKEHOUSE_GRACE_PERIOD_DAYS`), compact (bin-pack small files) and vacuum (delete obsolete files). Each step acquires the exclusive maintenance fence (`.LOCK`) and waits for in-flight append markers to drain.

::: ftm_lakehouse.operation.maintenance.OptimizeJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.OptimizeOperation
    options:
        heading_level: 3
        show_root_heading: true

## ShardOperation

Change the dataset's shard count after the fact: drain the journal, rewrite every `(bucket, origin)` group into the new shard partitions (streamed, one atomic Delta commit per group), then record the new count in `config.yml`. Neither dedupes nor sorts – it moves rows – so every rewritten partition comes out dirty and wants an `optimize` afterwards. Run it with writers stopped: the maintenance fence covers parquet appends, not journal writes, and a flush landing between the rewrite and the config write still resolves the old count.

::: ftm_lakehouse.operation.maintenance.ShardJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.ShardOperation
    options:
        heading_level: 3
        show_root_heading: true

## MigrateOperation

Apply the storage-layout migrations a dataset has not seen yet – the functions registered in `ftm_lakehouse.operation.migrations`, run in registry order and stamped with a `migrations/<function name>` tag each, so the function name is the migration id. Migrations are forward-only (no down-migration, no compatibility shim in the read path) and idempotent: `force` re-runs the whole registry, and a run that dies halfway resumes at the first untagged migration.

::: ftm_lakehouse.operation.maintenance.MigrateJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.MigrateOperation
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
