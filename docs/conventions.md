# Conventions

The path layout, artifact names and freshness tags are stable contracts: third-party tools can populate or consume a lakehouse by following them, without this library.

## Storage Layout

Layout of a lakehouse storage root, local or remote:

```
lakehouse/
└── {dataset}/
    ├── config.yml                # Dataset configuration (shards, compression, ...)
    ├── index.json                # Published dataset index with statistics
    ├── .LOCK                     # Exclusive maintenance fence
    ├── .LOCK-MERGE               # Merge / export-sweep lock
    │
    ├── archive/                  # Content-addressed file storage
    │   └── {ch[0:2]}/{ch[2:4]}/{ch[4:6]}/{checksum}/
    │       ├── blob              # Raw file content (stored once)
    │       ├── {file_id}.json    # File metadata (one per source path)
    │       └── {origin}.txt      # Extracted text (one per engine)
    │
    ├── statements/               # Delta Lake parquet store
    │   ├── _delta_log/
    │   └── shard={hex}/bucket={bucket}/origin={origin}/*.parquet
    │
    ├── entities.ftm.json[.gz|.zst]   # Aggregated entities export
    │
    ├── exports/
    │   ├── statements.csv[.gz|.zst]  # Statements export
    │   ├── statistics.json           # Entity counts, facets
    │   ├── parents.csv[.gz|.zst]     # Folders documents sit in, with their paths
    │   ├── documents.csv[.gz|.zst]   # Document metadata
    │   └── documents.{origin}.csv[..] # Document metadata, one origin only
    │
    ├── diffs/                    # Timestamped diff exports
    ├── versions/                 # Versioned snapshots (config, index, ...)
    │   └── YYYY/MM/{timestamp}/
    ├── tags/{tenant}/            # Freshness tags (default tenant: lakehouse)
    └── jobs/
        └── runs/{job_type}/{timestamp}.json
```

## Freshness Tags

Operations skip work their tags say is fresh: `is_latest(key, dependencies)` is `True` when `key` is newer than all its dependencies.

| Tag | Set by | Meaning |
|-----|--------|---------|
| `statements/last_updated` | Flush / append, `delete_origin` | The store's content moved – what exports, statistics and diffs depend on. A merge rewrites files, not content, and leaves it alone |
| `archive/last_updated` | Archiving a file | A file was archived |
| `exports/statements.csv`, `entities.ftm.json`, `exports/parents.csv`, `exports/documents.csv`, `exports/documents.{origin}.csv`, `exports/statistics.json`, `index.json` | `export` | Each artifact's key is its tag, stamped when a run wrote it. `archive download` (`DownloadArchiveOperation`) depends on `exports/documents.csv` |
| `operations/{name}/last_run` | `crawl`, `make`, `export`, `optimize`, `shard`, `migrate`, `download_archive` | When the operation last completed. `optimize` decides freshness from the store's dirty partitions, not from its tag |
| `migrations/{function}` | `maintenance migrate` | One per applied migration |

## Compression suffixes

With `compression` set in `config.yml` (`gz` / `zst`), the streamed artifacts carry the codec suffix – `entities.ftm.json.zst`, `exports/statements.csv.zst`, `exports/parents.csv.zst`, `exports/documents.csv.zst` – and `index.json` lists those names and urls. `index.json` and `statistics.json` are always plain JSON.

Diff directories stay codec-free (`diffs/exports/documents.csv/`) – they are named after the tag; only the files in them carry the suffix (`{timestamp}.diff.csv.zst`).

## Path conventions

Every path is built through `ftm_lakehouse.core.conventions.path`:

::: ftm_lakehouse.core.conventions.path
    options:
        heading_level: 3
        show_root_heading: false

## Tag conventions

::: ftm_lakehouse.core.conventions.tag
    options:
        heading_level: 3
        show_root_heading: false
