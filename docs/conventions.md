# Conventions

`ftm-lakehouse` is convention-driven: the path layout, artifact names and freshness tags are stable contracts – third-party tools can populate or consume a lakehouse by following them, without going through this library.

## Storage Layout

On-disk (or cloud storage) layout of a lakehouse storage root:

```
lakehouse/
└── {dataset}/
    ├── config.yml                # Dataset configuration (shards, compression, ...)
    ├── index.json                # Published dataset index with statistics
    ├── .LOCK                     # Exclusive maintenance fence
    ├── .LOCK-APPENDS/            # In-flight append markers (shared fence)
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
    │   ├── statements.csv[.gz|.zst]  # Sorted statements export
    │   ├── statistics.json           # Entity counts, facets
    │   └── documents.csv             # Document metadata
    │
    ├── diffs/                    # Timestamped delta diff exports
    ├── versions/                 # Versioned snapshots (config, index, ...)
    │   └── YYYY/MM/{timestamp}/
    ├── tags/{tenant}/            # Freshness tags (workflow state)
    └── jobs/
        └── runs/{job_type}/{timestamp}.json
```

## Freshness Tags

Operations use tags to track freshness and skip unnecessary work – `is_latest(key, dependencies)` returns `True` when `key` is newer than all its dependencies:

| Tag | Set by | Meaning |
|-----|--------|---------|
| `journal/last_updated` | Statement writes | Journal has uncommitted data |
| `journal/last_flushed` | Flush operation | Journal was flushed |
| `statements/last_updated` | Flush / append / merge | Parquet store was updated |
| `statements/last_optimized` | Optimize operation | Merge + compact + vacuum ran |
| `archive/last_updated` | File archive | New file was archived |
| `exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv`, `exports/statistics.json`, `index.json` | Export operations | Export target keys double as their freshness tags |
| `operations/crawl/last_run` | Crawl operation | Last crawl execution |

## Compression suffixes

When a dataset configures `compression` (`gz` / `zst` in `config.yml`), the streaming export artifacts carry the codec suffix – `entities.ftm.json.zst`, `exports/statements.csv.zst` – and `index.json` advertises the resulting names and urls. `index.json` and `statistics.json` themselves are always plain JSON.

## Path conventions

All path construction goes through `ftm_lakehouse.core.conventions.path` – rendered here so the constants stay in sync with the code:

::: ftm_lakehouse.core.conventions.path
    options:
        heading_level: 3
        show_root_heading: false

## Tag conventions

::: ftm_lakehouse.core.conventions.tag
    options:
        heading_level: 3
        show_root_heading: false
