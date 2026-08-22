# Architecture

This document describes the layered architecture of `ftm-lakehouse`.

## Overview

The codebase follows a strict layered architecture with clear separation of concerns – see [Module Layout](#module-layout) for the full tree.

## Dependency Rules

Layers can only depend on layers below them:

```mermaid
flowchart TD
    subgraph Public["Public API"]
        API["lake.py / catalog.py"]
    end

    subgraph Layer4["Layer 4"]
        OP[operation]
    end

    subgraph Layer3["Layer 3"]
        REPO[repository]
    end

    subgraph Layer2["Layer 2"]
        STORE[storage]
    end

    subgraph Layer1["Layer 1"]
        MODEL[model]
    end

    CORE[core]

    API --> REPO
    API --> OP
    API --> CORE
    OP --> REPO
    OP --> CORE
    REPO --> STORE
    REPO --> CORE
    STORE --> MODEL
    STORE --> CORE
```

## Layer 1: Model

Pure data structures with no dependencies. Pydantic models and lightweight typed primitives.

```
model/
  file.py        # File, Files - archived file metadata
  job.py         # JobModel, DatasetJobModel - job execution tracking
  dataset.py     # DatasetModel - dataset metadata / config
  statement.py   # SHARDED_SCHEMA (pyarrow) + TABLE (SQLAlchemy) +
                 # LakehouseStatement (LakeStatement + shard, deleted_at)
                 # + statements_to_arrow – schema for the parquet
                 # statement store and shared currency between buffer
                 # and writer.
```

**Principles:**

- No behavior beyond validation
- No storage awareness
- No external dependencies (except pydantic, pyarrow, sqlalchemy, anystore.model)

See [Model Reference](reference/model.md) for API details.

## Layer 2: Storage

Single-purpose storage interfaces. Each store does ONE thing.

```
storage/
  parquet.py         # ParquetStore - Delta Lake statement store
                     #   .append (sorted per-shard write)
                     #   .merge (per-partition dedup + tombstone reap)
                     #   .compact (file bin-pack)
                     #   .vacuum (delete obsolete files)
  journal/
    base.py          # BaseJournalStore
                     # .flush_batches()  – rotates the journal, yields
                     #                     Arrow batches, drops each segment
                     #                     once the consumer wrote it
    sql.py           # SqlJournalStore (sqlite / psql)
    api.py           # ApiJournalStore (HTTP forwarding)
  tags.py            # TagStore – key-value freshness tracking
  versions.py        # VersionStore – timestamped snapshots
```

Blob, file metadata, and text storage are handled directly by repositories using `anystore.Store` instances via `get_store()`, eliminating a layer of indirection.

### Sharded append-only pattern

The parquet statement store is partitioned by `(shard, bucket, origin)`:

- `shard` – `hash(entity_id) % shards` (the dataset's configured shard count), hex-padded
- `bucket` – coarse FtM schema group (thing / interval / document / page / pages / mention)
- `origin` – caller-supplied source tag

Each row carries `first_seen`, `last_seen`, `fragment`, and `deleted_at` directly in the parquet schema (no separate translog). The live `statement` query view is a plain `WHERE deleted_at IS NULL` scan – **no read-time dedupe** – so a filter (`schema` / `prop` / `entity_id`) pushes straight through to DuckDB's per-file statistics.

Writes are **append-only**: `append` sorts a per-shard batch by `(bucket, origin, entity_id, fragment, prop, id, last_seen DESC)` and writes one parquet file per `(shard, bucket, origin)` partition. Duplicates and tombstones land as additional rows.

**Correctness assumes an optimized store.** Dedupe, fragment supersession, `first_seen`/`last_seen` folding, and tombstone reaping all happen in `merge`; between a write and the next merge, reads can surface duplicate ids and rows whose delete has not been applied. Run `optimize` before you query or export. `merge` routes every row into one of two isolated branches on the `fragment` column (empty-string sentinel, never NULL):

- **non-fragment** (`fragment = ''`, the default): content-addressed dedup – latest `last_seen` per statement `id` wins; distinct ids never interact. Scoped per `(shard, bucket, origin)` partition, so the *same* statement observed under two origins is kept once per origin (merge cannot cross origin partitions).
- **fragment-bearing** (`fragment != ''`): supersession per `(origin, entity_id, prop, fragment)` group – every row tied at the group's max `last_seen` survives (the latest emission, multi-valued props included), older emissions go. See [Fragment Supersession](usage/entities.md#fragment-supersession) for semantics and the producer contract.

The async `optimize` operation produces this canonical state by running the three storage primitives in order. Each acquires the exclusive maintenance fence – the dataset-wide `.LOCK` plus a drain of in-flight append markers (`.LOCK-APPENDS/`) – so maintenance never races other maintenance or an append it could tombstone. Appends themselves only register a marker and run concurrently; Delta's optimistic concurrency serializes their commits:

| Step | Cost | What it does |
|------|------|--------------|
| `merge()` | expensive | Per-partition rewrite: keep latest row per id (`ROW_NUMBER`) / latest emission per fragment group, fold `first_seen` to min, drop tombstones past grace |
| `compact()` | cheap | Delta `OPTIMIZE compact` per partition – bin-packs small files |
| `vacuum()` | cheap | Delta `VACUUM` – delete files no longer referenced in the Delta log |

#### Sharding – why, and how many shards

The `shard` partition key is the unit that keeps per-partition working sets bounded, independent of total dataset size. Everything expensive in the lakehouse operates one `(shard, bucket)` partition at a time:

- **Writes:** statement rows arrive shard-sorted from the journal (a flush drains one rotated segment, ordered by shard), and `append` buffers at most one shard's batch in memory before writing it out.
- **Reads:** statement queries iterate `(shard, bucket)` partitions in Python and push `WHERE shard = ?` into DuckDB; the live view is a plain scan, so filters push to file statistics and a full-store `ORDER BY entity_id` stays bounded to one partition. Single-entity lookups hash the entity id and scan just its own shard.
- **Optimize:** the merge rewrite materializes one partition at a time – its memory and rewrite cost scale with the largest partition, not the whole table.

Sharding is a trade-off, not a free win: every shard multiplies the partition count (`shard × bucket × origin`), which means more small parquet files, more Delta log metadata, and more per-partition query iterations. For small and medium datasets that overhead costs more than the bounded working sets gain.

That's why the **default is `0`** – a single shard (`shard <= 1` collapses to one `"0"` partition). The default is hardcoded, deliberately not an environment setting: the shard count is per-dataset configuration, recorded in the dataset's `config.yml` at creation (e.g. `ensure_dataset("big_leak", shards=8)`), and every reader and writer resolves it from there – a process running with a different environment cannot mis-shard an existing dataset. Don't configure shards unless the dataset is huge: from roughly tens of millions of statements upward, set `shards: 8` (or more, scaling with entity count) so merge rewrites stay bounded. The shard count is **immutable after the first write** – changing it requires a full rewrite of the statement store, so size it for the data you expect, not the data you have on day one.

**Principles:**

- Each store is independent – no cross-store awareness
- Operates on a single storage URI
- Returns/accepts model objects
- No business logic

See [Storage Reference](reference/storage.md) for API details.

## Layer 3: Repository

Domain-specific combinations of multiple stores. Each repository owns ONE domain concept.

```
repository/
  base.py        # DatasetHandle - dataset-addressed handle base
  archive.py     # ArchiveRepository - blobs, file metadata, text (via get_store)
  entities.py    # EntityRepository - uses JournalStore + ParquetStore
  documents.py   # DocumentRepository - compiled document metadata CSV + diffs
  job.py         # JobRepository - job tracking (via get_store)
  factories.py   # Cached factory functions (get_archive, get_entities, etc.)
```

**Principles:**

- Combines stores for a single domain concept
- May use `get_store()` directly for simple storage needs (blobs, metadata JSON)
- No cross-domain awareness (ArchiveRepository doesn't know about statements)
- Provides domain-specific operations
- Uses TagStore for freshness tracking

See [Repository Reference](reference/repository.md) for API details.

## Layer 4: Operation

Multi-step workflows that coordinate across repositories. This is where "action chains" are made explicit.

```
operation/
  base.py          # DatasetJobOperation - base class with freshness checks
  export.py        # ExportOperation - all exports, dispatched by ExportKind
  crawl.py         # CrawlOperation - source → files → entities
  maintenance.py   # OptimizeOperation - merge + compact + vacuum in one pass
  make.py          # MakeOperation - flush + all exports + index
  download.py      # DownloadArchiveOperation
```

**Principles:**

- Operations are internal (not exposed to clients directly)
- Make multi-step processes explicit
- Handle freshness checks via `@skip_if_latest` decorator or `ensure_flush()`
- May span multiple repositories
- Create job run records for tracking

See [Operation Reference](reference/operation.md) for API details.

## Layer 5: Public API

The public interface that clients use – repositories are the dataset handle, resolved through the LRU-cached factories:

```
lake.py          # get_lakehouse(), repository shortcuts (re-exports)
catalog.py       # config.yml lifecycle functions + slim Catalog
```

**Day-to-day access** goes through the repository factories – every path addressing the same dataset shares one cached instance:

```python
from ftm_lakehouse import ensure_dataset, get_entities, get_archive

ensure_dataset("my_data", shards=8, compression="zst")   # config recorded at creation
entities = get_entities("my_data")                       # EntityRepository
archive = get_archive("my_data")                         # ArchiveRepository
```

**Config lifecycle** lives in module functions: `ensure_dataset()` (get-or-create), `update_dataset()` (merge-write + versioned snapshot; invalidates the factory caches so newly fetched repositories see the fresh config), `get_dataset_model()` (fresh read), `get_dataset_index()`, `dataset_exists()`. Repositories snapshot their model (`shards`, `compression`) at construction – layout-affecting config must be set at creation.

**Multi-dataset concerns** go through the slim `Catalog` (`get_lakehouse()`): `list_datasets()`, `dataset_uri(name)`. The API server keeps one as `app.state.lake`.

**Custom dataset models**: register a `DatasetModel` subclass process-wide via `set_model_class()` – every config read constructs through it.

See [Lake Reference](reference/lake.md) for API details.

## Core

Cross-cutting concerns used by all layers.

```
core/
  settings.py           # Configuration from environment (Settings, ApiSettings)
  config.py             # Config loading utilities (load_config)
  conventions/
    path.py             # Path patterns (archive/, exports/, etc.)
    tag.py              # Tag keys (journal/last_updated, exports/statements, etc.)
```

**Principles:**

- No business logic
- Pure utilities and configuration
- Can be used by any layer

**Additional Modules:**
```
helpers/                # Domain-specific utilities
  file.py               # File handling (mime_to_schema, etc.)
  statements.py         # Statement pack/unpack for journal
  serialization.py      # Model serialization utilities
```

## Usage Examples

For detailed usage examples, see:

- [Quickstart](quickstart.md) - Getting started guide
- [Working with Entities](usage/entities.md) - Entity/statement operations
- [Working with Files](usage/archive.md) - File archive operations

## Module Layout

```
ftm_lakehouse/
├── lake.py                  # get_lakehouse(), repository shortcuts
├── catalog.py               # config lifecycle fns + slim Catalog
├── util.py                  # dependency-light primitives (validation, checksums)
├── exceptions.py
│
├── model/                   # Layer 1: Pure data structures
│   ├── dataset.py           # DatasetModel + set_model_class hook
│   ├── file.py              # File metadata model
│   ├── job.py               # Job models
│   └── statement.py         # SHARDED_SCHEMA, LakehouseStatement
│
├── storage/                 # Layer 2: Single-purpose storage interfaces
│   ├── journal/             # SQL write-ahead log (sql.py, api.py, base.py)
│   ├── parquet.py           # ParquetStore (Delta Lake, write fence, merge)
│   ├── tags.py              # TagStore (freshness)
│   └── versions.py          # VersionStore (config / index snapshots)
│
├── repository/              # Layer 3: Domain-specific storage combinations
│   ├── base.py              # DatasetHandle, dataset_uri(), ensure_zfs()
│   ├── factories.py         # LRU-cached single instantiation path
│   ├── entities/            # EntityRepository (main.py) + API delegate (api.py)
│   ├── archive.py           # ArchiveRepository (content-addressed files)
│   ├── documents.py         # DocumentRepository
│   ├── diff.py              # Delta diff export mixin
│   └── job.py               # JobRepository
│
├── operation/               # Layer 4: Multi-step workflow operations
│   ├── base.py              # DatasetJobOperation (freshness targets / deps)
│   ├── factories.py         # export(), optimize(), make(), crawl(), ...
│   ├── export.py            # ExportOperation (spec table per kind)
│   ├── maintenance.py       # OptimizeOperation
│   ├── make.py              # MakeOperation (full workflow)
│   ├── crawl.py             # CrawlOperation
│   └── download.py          # DownloadArchiveOperation
│
├── logic/                   # Pure business logic (no storage deps)
│   ├── entities/            # aggregate.py, buffer.py, explode.py
│   ├── parquet.py           # DuckDB view / merge SQL builders
│   └── compress.py          # Streaming (de)compression (gz / zst)
│
├── helpers/                 # FtM-domain building blocks
│   ├── statements.py        # Statement wire format, BASE_ID stub
│   ├── file.py              # File → entity construction
│   └── serialization.py
│
├── api/                     # FastAPI REST API
│   ├── main.py              # App factory, blob mounting
│   ├── dependencies.py      # DatasetName / Entities / Shards / Journal deps
│   └── routes/              # entities.py, journal.py, operations.py
│
├── cli/                     # Typer CLI (sub-typer groups)
│   ├── __init__.py          # Main app, contexts, ls / datasets / configure
│   ├── io.py                # Shared bulk-import loop
│   ├── entities.py          # entities iterate / stream / import
│   ├── statements.py        # statements iterate / stream / import / sql
│   ├── archive.py           # archive get / ls / download
│   ├── maintenance.py       # make, export, maintenance flush / optimize / unlock
│   ├── crawl.py             # crawl (top level)
│   └── zfs.py               # zfs init (agent lives in the zfs-agent package)
│
└── core/                    # Cross-cutting concerns
    ├── settings.py          # LAKEHOUSE_* env configuration
    ├── config.py            # config.yml loading
    ├── api.py               # API-mode delegation mixin
    ├── conventions/         # path.py, tag.py
    └── zfs.py               # ZFS tuning + zfs-agent package caller
```

## Storage Layout & Tags

The on-disk layout of a dataset and the freshness-tag vocabulary are documented in [Conventions](conventions.md).

## Dependency Chain

```mermaid
flowchart TD
    A[Tenant writes entities] --> B[(Journal)]
    A2[Tenant archives files] --> AR[(Archive)]
    AR -.-> T0[archive/last_updated]
    AR --> |"create Document"| B

    B --> |"flush()"| C[(Parquet Store)]
    A3[Tenant bulk imports] --> |"EntityBuffer + write_batches"| C

    C --> |"optimize() – merge + compact + vacuum"| C

    C --> |"export(statements)"| D[statements.csv]
    C --> |"export(entities)"| E[entities.ftm.json]
    C --> |"export(statistics)"| F[statistics.json]
    F --> |"export(index)"| G[index.json]

    B -.-> T1[journal/last_updated]
    B -.-> T1b[journal/last_flushed]
    C -.-> T2[statements/last_updated]
    C -.-> T2a[statements/last_optimized]
    D -.-> T3[exports/statements]
    E -.-> T4[exports/entities_json]
    F -.-> T5[exports/statistics]

    classDef tag fill:#f9f,stroke:#333,stroke-width:1px
    classDef storage fill:#69b,stroke:#333,stroke-width:2px,color:#fff
    class T0,T1,T1b,T2,T2a,T3,T4,T5 tag
    class B,C,AR storage
```

## Key Principles

1. **Each storage does ONE thing** - no cross-storage awareness
2. **Repositories combine storages** - for ONE domain concept
3. **Operations are explicit workflows** - no hidden side effects
4. **Freshness is explicit** - checked in operations, not decorators
5. **Public API is simple** - delegates to repositories/operations
6. **`__init__.py` exports only** - no logic in init files
7. **Strict layer dependencies** - upper layers depend on lower layers only
