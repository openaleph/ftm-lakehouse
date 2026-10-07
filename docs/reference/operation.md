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

One operation for every export: `exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv` and `exports/documents.crawl.csv` (crawled files only), each with its diff series, and `exports/statistics.json` – all from a **single pass** over the statement store – then `index.json`, which registers them.

The sweep runs per `(shard, bucket)` pair, in `LAKEHOUSE_WORKERS` processes, into *parts* of every artifact that are concatenated as encoded – so a compressed artifact is a multi-frame zstd or multi-member gzip file. The stdlib file classes (`GzipFile`, `ZstdFile`, …), and so every reader here, read it as one; `zlib.decompress(blob, 31)` returns only the first member. A crashed export leaves the previous artifacts intact.

Diff entries follow the [OpenSanctions delta format](https://www.opensanctions.org/docs/bulk/delta/): `ADD` (every statement new), `MOD` (predates the window, changed in it), `DEL` (gone). `ADD` and `MOD` carry the entity whole.

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

::: ftm_lakehouse.repository.artifacts.DiffOp
    options:
        heading_level: 3
        show_root_heading: true

## OptimizeOperation

Optimize the parquet statement store in one pass: merge (rewrite every dirty partition into one canonical file – collapse duplicates, fold `first_seen` to the min, `last_seen` to the max, drop tombstones older than the grace cutoff per `LAKEHOUSE_GRACE_PERIOD_DAYS`) and vacuum (delete the files that replaced). Reads reconcile un-merged rows, so this is an optimisation, not a precondition. `merge` holds the merge lock (`.LOCK-MERGE`), which appends do not wait for, so ingest flows through it; `vacuum` takes the exclusive fence (`.LOCK`) as well.

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
