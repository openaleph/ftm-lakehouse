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

One pass over the statement store writes every export: `exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv` and `exports/documents.crawl.csv` (crawled files only) with their diff series, `exports/parents.csv` (every folder a document can sit in, with its path) and `exports/statistics.json`. Then `index.json` registers them.

The `(shard, bucket)` pairs are swept in `LAKEHOUSE_WORKERS` processes. Each finished pair's encoded parts are appended to a temporary file beside its artifact, which is moved into place once every pair is in – a crashed export leaves the previous artifacts intact. A compressed artifact is therefore a multi-frame zstd or multi-member gzip file: `GzipFile` / `ZstdFile` read it whole, `zlib.decompress(blob, 31)` only its first member.

The entities diff follows the [OpenSanctions delta format](https://www.opensanctions.org/docs/bulk/delta/): `ADD` (every statement new), `MOD` (predates the window, changed in it), `DEL` (gone); `ADD` and `MOD` carry the whole entity. A documents diff is the csv with a leading `op` column.

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

`merge` rewrites every dirty partition into one file – duplicates collapsed, `first_seen` folded to the min and `last_seen` to the max, tombstones older than `LAKEHOUSE_GRACE_PERIOD_DAYS` dropped – then `vacuum` deletes the replaced files. Reads reconcile un-merged rows, so this is an optimisation, not a precondition. `merge` holds only the merge lock (`.LOCK-MERGE`), so appends keep flowing; `vacuum` takes the exclusive `.LOCK` as well.

::: ftm_lakehouse.operation.maintenance.OptimizeJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.OptimizeOperation
    options:
        heading_level: 3
        show_root_heading: true

## ShardOperation

Change a dataset's shard count: drain the journal, rewrite each `(bucket, origin)` group onto the new shards (streamed, one Delta commit per group), then record the count in `config.yml`. Rows are moved, not deduped, so every partition comes out dirty – run `optimize` afterwards. Run it with writers stopped: `.LOCK` holds off parquet appends, not journal writes, and a flush between the rewrite and the config write would still use the old count.

::: ftm_lakehouse.operation.maintenance.ShardJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.ShardOperation
    options:
        heading_level: 3
        show_root_heading: true

## MigrateOperation

Apply the migrations registered in `ftm_lakehouse.operation.migrations` that a dataset has not seen, oldest first, stamping a `migrations/<function name>` tag for each – the function name is the migration id. Migrations are forward-only and idempotent: a run that dies resumes at the first untagged one, and `force` re-runs them all.

::: ftm_lakehouse.operation.maintenance.MigrateJob
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.operation.MigrateOperation
    options:
        heading_level: 3
        show_root_heading: true

## MakeOperation

Flush the journal, then export. The `make` CLI runs `optimize` in between by default.

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
