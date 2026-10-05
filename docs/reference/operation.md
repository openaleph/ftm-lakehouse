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

One operation for every export. A run writes each artifact that is a function of the entity stream from a **single pass** over the statement store – `exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv` and `exports/documents.crawl.csv` (scoped to crawled files), each with its own diff series, plus `exports/statistics.json`, whose counts are folded from the same stream. A diff entry costs nothing extra: the payload a diff publishes is the payload the export just wrote, so it is emitted from the same loop rather than re-read afterwards.

The sweep is one Python thread holding the GIL – Arrow batches into row dicts into the fold into the artifacts – so with `LAKEHOUSE_WORKERS` above one it fans the store's `(shard, bucket)` pairs out to worker processes instead. The pairs are independent, because an entity id is placed in exactly one of them, so each worker folds its own entities into *parts* of every artifact and the parent concatenates them: the parts already carry the dataset's codec, so the assembled artifact is their frames copied verbatim – a multi-frame zstd or multi-member gzip file, which the codec reads back as one.

That framing is invisible to every reader in this codebase and to the standard library's: `Store.open(key, "rb", compression=...)`, the `smart_stream_*` helpers and `Artifact.reader` all layer the stdlib file classes (`GzipFile`, `BZ2File`, `LZMAFile`, `ZstdFile`), which are streaming multi-member readers, and `gzip.decompress` / `compression.zstd.decompress` read across frames too. What does *not* is a consumer decoding at the `zlib` level – `zlib.decompress(blob, 31)` returns the first member and silently drops the rest.

What the parent keeps is what a pair cannot answer on its own: one Delta snapshot, so every worker reads the same version of the store; the single delete-candidate scan, since a delete never comes past a live-view sweep (each worker is handed its *shard's* candidates and reports the ones it met alive, and whatever is left over is gone); the folder tree, because a document's ancestors are placed by their own ids and sit in other workers' pairs, so the workers stage their rows and the parent resolves and writes `documents.csv` whole; the statistics fold, merged from the workers' collectors; and every tag write. A worker's artifacts are therefore only *parts* until the parent assembles them – which also means a crashed parallel export leaves the previous artifacts intact.

`index.json` is the one artifact that is not a function of the stream – it is store metadata registering what the others produced, so it is written after the sweep, in the same run. `ExportKind` is what each artifact is called – the name it is addressed by and the key it is reported under in the run's result.

Diff entries carry one of three ops, per the [OpenSanctions delta format](https://www.opensanctions.org/docs/bulk/delta/): `ADD` for an entity whose every statement is new, `MOD` for one that predates the diff window and changed in it, and `DEL` for one that is gone. `ADD` and `MOD` both carry the entity whole, so a consumer indexes either the same way.

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
