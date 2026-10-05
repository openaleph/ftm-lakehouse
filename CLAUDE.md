# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Rules

1. Don’t assume. Don’t hide confusion. Surface tradeoffs.
2. Minimum code that solves the problem. Nothing speculative.
3. Touch only what you must. Clean up only your own mess.
4. Define success criteria. Loop until verified.

## Project Overview

`ftm-lakehouse` is a Python library providing data standard, archive storage, and retrieval for leaked data and document collections. It uses the FollowTheMoney data model for structured entity data and provides multi-tenant storage with support for local filesystems and S3-compatible object storage.

## Development Environment

**Important**: Always use the virtualenv at `.venv` when running commands. Either activate it or use `.venv/bin/` prefix:

```bash
# Option 1: Activate the virtual environment
source .venv/bin/activate

# Option 2: Use .venv/bin/ prefix directly (preferred for Claude)
.venv/bin/pytest -v
.venv/bin/python -m ftm_lakehouse

# Option 3: Use poetry run (if poetry is available)
poetry run <command>
```

## Common Commands

```bash
# Install dependencies (requires poetry)
poetry install --with dev --all-extras

# Full test suite: spins up the docker compose stack, runs two pytest passes
# (local+api variants, then docker variants against nginx), tears the stack down
make test

# Plain pytest – docker-variant fixtures auto-skip when the stack isn't running
poetry run pytest -v --capture=sys

# Run a single test file
poetry run pytest tests/test_unit_util.py -v

# Run a specific test
poetry run pytest tests/test_unit_util.py::test_function_name -v

# Docker compose stack (postgres + lakehouse + nginx on :8000)
make start
make stop

# Type checking
make typecheck

# Linting
make lint

# Pre-commit hooks (must be installed first)
poetry run pre-commit install
poetry run pre-commit run -a

# Run local API server (granian, port 5000, autoreload)
make api

# Build documentation (zensical, not plain mkdocs)
.venv/bin/zensical build

# Serve documentation locally
.venv/bin/zensical serve
```

## Documentation conventions

### Docstrings

Use Google-style docstrings with the canonical section order so mkdocstrings / zensical renders them consistently:

```python
def merge(self, grace_period_days: int | None = None) -> int:
    """One-line summary, imperative voice.

    Longer description, multi-line if needed. Cross-link other things with
    mkdocs-autorefs: [`LakehouseStatement`][ftm_lakehouse.model.statement.LakehouseStatement],
    [`flush`][ParquetStore.flush].

    Args:
        grace_period_days: Override ``settings.grace_period_days``. Pass ``0``
            to drop tombstones immediately.

    Returns:
        Number of statements merged.

    Yields:
        (only for generators) ``LakehouseStatement``.

    Raises:
        RuntimeError: when the dataset write fence cannot be acquired.
    """
```

- Use ``Args``, ``Returns`` / ``Yields`` (only one, matching the function), ``Raises`` – in that order. ``Example:`` / ``Examples:`` / ``Attributes:`` are the other recognized sections; anything else (``Usage:``) is not a section and renders as loose text.
- Indent the body of each section by 4 spaces under the section header.
- Backtick code/identifiers in prose (``` ``foo`` ```). Sphinx roles (``:class:`` / ``:meth:`` / ``:data:``) are **not** interpreted – they render literally – so cross-link with mkdocs-autorefs instead: ``[`display`][identifier]``.
- ``scoped_crossrefs`` is on, so ``identifier`` may be the short form when its first component is resolvable in the module's own scope (``[`merge`][ParquetStore.merge]`` inside ``storage/parquet.py``); otherwise use the full dotted path (``[`append`][ftm_lakehouse.storage.parquet.ParquetStore.append]``).
- Only identifiers mkdocstrings actually renders can be linked – check ``site/objects.inv``. Private members, undocumented classes (``EntityBuffer``, ``BaseJournalWriter``, ``RowBuffer``) and external types (``ftmq.store.lake.LakeStatement``) get plain backticks; an unresolved link warns at build time and renders as raw markdown.
- ``.venv/bin/zensical build`` must end with "No issues found" (delete ``.cache`` first – it caches the collected docstrings).

### Dashes

Use the **en-dash** ``–`` (U+2013) for parenthetical asides and ranges. Do **not** use the em-dash ``—`` (U+2014). Plain ASCII hyphen-minus ``-`` is fine for compound words and CLI flag examples.

Apply the same rule to user-facing docstrings, markdown docs, and CLI help text.

### Markdown line wrapping

Do **not** hard-wrap prose in markdown files (`.md`). Each paragraph stays on a single logical line so diffs stay clean and editors / renderers handle wrapping. This applies to docs under `docs/`, top-level READMEs, and CLAUDE.md itself. Code blocks, tables, and list markers are unaffected.

## Architecture

The codebase follows a strict layered architecture with clear separation of concerns:

```
ftm_lakehouse/
├── lake.py              # Public convenience functions (get_lakehouse, get_entities, etc.)
├── catalog.py           # Dataset config lifecycle fns + slim Catalog
│
├── model/               # Layer 1: Pure data structures (Pydantic models)
├── storage/             # Layer 2: Single-purpose storage interfaces
├── repository/          # Layer 3: Domain-specific storage combinations
├── operation/           # Layer 4: Multi-step workflow operations
│
├── helpers/             # Domain-specific utilities
├── logic/               # Business logic
├── api/                 # FastAPI REST API
│
├── cli/                 # Typer CLI commands – sub-typer groups
│   ├── __init__.py      # Main app, callback, ls/datasets/configure, contexts,
│   │                    #   `sub_typer` group factory + shared `OPT_*` option
│   │                    #   constants + `write_config` helper shared with `make -c`
│   ├── io.py            # Shared bulk-import loops (`_bulk_import`,
│   │                    #   `_bulk_import_rows`) + `stream_export`
│   ├── maintenance.py   # `maintenance` group (optimize, shard, unlock)
│   │                    #   + top-level `make` / `export` shortcuts
│   ├── crawl.py         # top-level `crawl` command
│   ├── entities.py      # `entities` group (iterate, stream, import)
│   ├── statements.py    # `statements` group (iterate, stream, import, sql)
│   ├── archive.py       # `archive` group (get, head, ls, download)
│   └── zfs.py           # `zfs` group (init)
│
└── core/                # Cross-cutting concerns
    ├── settings.py      # Configuration (LAKEHOUSE_* env vars)
    ├── config.py        # Config loading utilities
    ├── api.py           # Outgoing lakehouse-api client + `no_api` guard
    ├── arrow.py         # Arrow IPC framing for the api wire
    ├── conventions/     # Path and tag conventions
    └── zfs.py           # ZFS tuning + zfs-agent package caller
```

### Layer Dependencies

Layers can only depend on layers below them:

- **Public API** (lake.py, catalog.py) → Repository, Operation, Core
- **Operation** → Repository, Core
- **Repository** → Storage, Core
- **Storage** → Model, Core

Below the layers sit two utility tiers with a strict rule:

- **`util.py`** – dependency-light primitives (name/path validation, checksums, templating) at the very bottom; importable from anywhere including `core/` and `model/`, and must not import FtM or any domain module.
- **`helpers/`** – FtM-domain building blocks (statement wire format, file/folder entity construction, model serialization); may import `util.py`, `core/` and external FtM libraries, never `model/` or higher layers.

### Key Components

- **Repositories are the dataset handle** – resolved through the LRU-cached factories (`repository/factories.py`), the single instantiation path shared by library callers, CLI, operations and the API server. Api mode is a construction-time pick, not per-call branching: for http uris the entities factory returns `ApiEntityRepository` (subclass overriding the api-capable methods under their public names), mirroring how `get_journal` picks `ApiJournalStore`. Direct `EntityRepository(name, http_uri)` construction raises – go through the factory:
  - `get_archive("name")`: File storage (ArchiveRepository)
  - `get_entities("name")`: Entity/statement operations (EntityRepository)
  - `get_documents("name")`: Document metadata (DocumentRepository)
  - Job runs are per job class – `repository.factories.get_jobs(name, JobClass)`
  - `repository.base.dataset_uri(name, uri)` canonicalizes AND validates the name – no caller-supplied name reaches path construction unchecked.
- **Config lifecycle** (`catalog.py` module fns): `ensure_dataset` (get-or-create), `update_dataset` (merge-write + versioned snapshot; calls `factories.clear_caches()` so newly fetched repos see fresh config – held instances keep their snapshot), `get_dataset_model` (fresh read), `get_dataset_index`, `dataset_exists`. Repositories snapshot `_model` (shards, compression) at construction; layout-affecting config must be set at creation – `shards` is the one exception, changeable after the fact by `ShardOperation`, which rewrites the store *before* writing the new count. Custom `DatasetModel` subclasses register process-wide via `set_model_class()` (module hook, no generics).
- **Catalog** (slim): `list_datasets()` + `dataset_uri(name)`; the API server keeps one as `app.state.lake`. There is no Dataset class – the former `Dataset`/`get_dataset` surface was removed pre-release.
- **API app** (`api/main.py`): lakehouse routes live under `/{dataset}/_api/...`; blob storage is served by mounting the whole putfs Starlette app at `/` when the lake URI is a local path (its catch-all `/{key:path}` sits behind the `_api` routes), or anystore's `archive_router` for other backends. `ValueError` → 400, `DoesNotExist` → 404 via exception handlers.

### Data Flow

1. **Writing (journal-backed)**: Entities → `EntityBuffer` → SQL Journal (`JOURNAL_SCHEMA` – the parquet columns minus `shard`) → (via `flush`, as Arrow tables) → Parquet Store, which derives `shard` on append
2. **Writing (direct bulk)**: Entities → `EntityBuffer` (in-memory, keyed by `(id, origin, fragment, role)`) → `buffer.flush_table()` (one packed table) → `repo.write_batches` → Parquet Store. With `--unsafe` on the CLI import commands: payload dicts → `logic/entities/explode.py` (packed row dicts, no FtM object construction; parity-tested against the safe path incl. statement ids and namespace stripping) → `RowBuffer` (flushes a `JOURNAL_SCHEMA` table) → `repo.write_batches` → Parquet Store
3. **Querying**: every read runs over the files of one `(shard, bucket)` pair, taken from the Delta snapshot each process keeps (`_current_snapshot`, advanced with `update_incremental`, shared with appends under `_snapshot_lock`) – never `delta_scan`. `_scoped_sources` yields the pair's partitions as one relation (`partition_source_sql`) plus whether every one of them is *clean* (all active files named `merged-*`, `MERGED_PREFIX`, i.e. written by `merge`); `_cursor_over` registers temp `statement_raw` / `statement` views on a cursor, `statement` being a plain `deleted_at IS NULL` scan (`live_rows_sql`) for a clean source and the dedupe query (`dedupe_rows_sql` = `_dedupe_sql`) otherwise – so reads are correct on any store and cheapest on a merged one. An id-pruned read puts every pair it can touch into one query. Sorted/sliced queries run the same way over `delta_scan`; `stats()` and the raw-SQL CLI use ftmq's `LakeStore` connection-level views, where `statement` always reconciles. The dedupe query routes rows into two isolated branches on `fragment`: non-fragment rows (`fragment = ''`) dedupe per `(statement id, role)`, fragment rows supersede per `(origin, entity_id, prop, fragment, role)` group (latest emission wins, ties survive together – see `docs/usage/entities.md#fragment-supersession`); `entity_id` sits in both window keys so an id lookup pushes below the windows. `role` sits in every window key including the `first_seen` fold, so a role's first assertion of content another role already wrote keeps its own date and stays diffable. Filters reach SQL by compiling an ftmq `Query` (node DSL – `M` meta / `P` property / `G` group / `C` context, not flat kwargs) through a lakehouse `SqlSource` (`make_source` in `storage/parquet.py`; one for the live view, one for `statement_raw`) – keyed on `entity_id` (no physical `canonical_id`) with two partition prunes folded into every clause: schema→`bucket` and `entity_id`→`shard` (`make_prune_by_shard`); reads skip the `(shard, bucket)` pairs a prune excludes (`_prune_values`, ftmq's flat-AND soundness rule). `origin` and `role` are ordinary `Query` nodes like any other filter (`C(role="user:42")` – any `SHARDED_SCHEMA` column is `C`-filterable, checked at compile time). The HTTP query endpoints carry the whole query as a JSON dict (`Query.to_dict` / `from_dict`), with `flush_first` as the only sibling body field.
4. **Maintenance (async)**: nothing downstream needs a merged store – reads reconcile – so `merge` is an optimisation (a clean partition reads as a plain scan) and the disk reclaim (tombstones past grace and the rows they shadow go). `DatasetJobOperation.prepare()` runs before the freshness window opens (`Tags.touch` stamps the target with its *entry* time, so preparing inside the window would backdate the result); `MakeOperation.prepare` and `ExportOperation.prepare` flush the journal there – a `LIMIT 1` probe when it is empty – and both depend on `statements/last_updated`, the one content clock (appends and `delete_origin` move it, a merge does not), so a scheduled `make` converges and stays fresh across an optimize. `MakeOperation` never merges; the `make` CLI runs `optimize` first by default. `OptimizeOperation` (target `operations/optimize/last_run`) overrides `is_fresh()` with `ParquetStore.needs_merge` – any partition holding a file merge did not write – and runs the two storage primitives in order: `merge` (per-partition dedup + tombstone reap with grace, one `merged-*` file per partition, `dataChange=false` commits in batches of `MERGE_COMMIT_BATCH`, a checkpoint at the end) and `vacuum` (drop the replaced files); `ParquetStore.shard` is the third, moving rows *between* partitions. Two lock files: the exclusive `.LOCK` is taken by the in-place rewrites (re-shard, `delete_origin`, schema changes, vacuum) and appends back off while it is held (`_await_unlocked`, no marker of their own); `merge` and the export sweep take `.LOCK-MERGE` instead (`merge_lock`), which the exclusive ops take too – so ingest keeps flowing through a merge (a merge removes exactly the files it read, an append only adds, Delta commits both, the read reconciles), and an optimize cannot vacuum files a running sweep still names. Appends run concurrently, Delta's optimistic concurrency serializing their commits. Table creation is one empty commit under the exclusive lock (`ParquetStore._ensure_table`). All waits bounded by `LAKEHOUSE_LOCK_MAX_RETRIES`; stale locks need `ftm-lakehouse maintenance unlock`. Exports and stats assume an optimized store. `MigrateOperation` (`maintenance migrate [--all]`, run by `docker-entrypoint.sh` on container startup) is separate from the optimize trio: it applies the forward-only, idempotent functions registered in `operation/migrations.py`, oldest first, stamping one `migrations/<function name>` tag per migration – the function name is the migration id, and a half-finished run resumes at the first untagged one. `ParquetStore.evolve_schema` is the primitive behind the current entry: a metadata-only Delta commit adding the `SHARDED_SCHEMA` columns an older table lacks (no file rewritten, missing columns read NULL, nothing owed a re-merge). Additive only – a *removed* column needs a full rewrite.
5. **Exporting**: one sweep over the entity stream writes every artifact that is a function of it. `ExportOperation.export` runs `ParquetStore.sweep` – a single `_execute_partitioned(statement_csv_select())` scan whose Arrow batches are teed into the `pyarrow.CSVWriter` for `statements.csv` *and* through `batch.to_pylist()` into `aggregate_unsafe` – then hands each `EntityPayload` to every artifact the run covers. Artifacts are declared one class per artifact in `repository/artifacts.py` (`Artifact` / `DiffableArtifact` / `VersionedArtifact`), each carrying its key, mime type, dependencies, codec policy and `ExportKind`; `ArtifactsRepository` binds them to a dataset. The split everything rests on is `Artifact.tag` (codec-free – the freshness tag, and what a diff series is named after) vs `Artifact.key` (carries the dataset codec – what is on disk). An `Artifact` is stateless; everything true only during a run – open writers, diff window, counters – lives on an `ArtifactRun` (`EntitiesRun` / `DocumentsRun`), and `ExportSession` drives them as one loop, so the fan-out is one polymorphic `consume(payload)` rather than per-artifact branching. `EntityPayload.to_dict()` is memoised so several runs share one dict. Documents are written per scope in `DOCUMENT_ORIGINS` – every origin, plus `exports/documents.crawl.csv`, selected by *membership* (`payload.origins`), each document described whole. A document's `path` is not knowable during the sweep – it is the chain of its ancestors' names, and those come past as entities like any other, in no order – so `DocumentsRun` is two-phase: `consume` stages one json line per entity, carrying the row with its `parent` ids in place of a path (`doc`) and / or the name it appears in a path as (`folder`, for anything `is_parent_schema` admits – today the `Folder` schemata, staged whatever the scope, so an origin-scoped csv still resolves through another origin's parents), and `finish` builds the `FolderTree` out of those same staged rows (`resolve`) before sweeping the file into the csv. The tree comes from the staged rows rather than a structure filled beside them because any document may become another's parent, and then the staged rows already are every potential parent there is. Staging is a local json-lines file (`TMPDIR`), so the second pass is over the dataset's documents – where asking the store for the tree up front was a pass over every statement in the `document` bucket of every shard, and `schema` sits in no window key of `_dedupe_sql`, so on an unmerged store the dedupe resolved before it could filter. The folder tree exists to be written into the csv, so there is no query for it any more – `DocumentRepository` is the read side (`stream`, `deleted_ids`). A run writes every artifact `ArtifactsRepository.streamed()` yields – all of them bar `index.json` – and `ExportKind` is what each one is called (`Artifact.kind` → `Artifact.name`, the keys of the result dict). `statistics.json` is streamed too – `StatisticsRun` folds the counts out of the same entities (`logic/entities/stats.py`, `StatsCollector` → ftmq's `compile_stats`) and writes them in `finish`, where asking the store (`stats()`) costs six aggregate queries over every row of the reconciling view (measured 1.8x slower standalone, and free when the sweep runs anyway – `make` used to pay both). It follows the SQL path's semantics so published numbers don't move: an entity counts in *every* group its schema belongs to (`Message` / `Event` / `Project` / `Trip` / `CallForTenders` are both a `Thing` and an `Interval`, so the group totals are not a partition of `entity_count`, and `Page` / `Mention` / `Similar` are in neither), `start` / `end` span every date-typed prop the FtM model does not mark `hidden`, as stored ISO strings (`_date_props`, read per schema – today that excludes exactly `processedAt`, the ingestion timestamp, which would otherwise pin `end` to the last crawl; this is the one number the sweep and `stats()` differ on, since the SQL path has no such notion), and a country counts once per entity. The one thing a stream cannot reproduce is the SQL path's per-*statement* schema grouping. With `LAKEHOUSE_WORKERS > 1` the sweep fans `(shard, bucket)` pairs out to spawned processes (`export_partition`): a worker reuses the whole fan-out with its writers pointed at a part directory (`Artifact.part`, threaded through `writer`/`diff_writer`/`run`/`session` – never through `key`, which is identity), prepares and closes its session but never finishes or commits it, and returns its counts, the DEL candidates it met alive and its `StatsCollector`. The parent pins one snapshot and ships relation SQL (`sweep_sources` → `pair_source`), so no worker replays the Delta log; it keeps `load_pending` (splitting candidates by `entity_shard` – by shard, not pair, since an id names its shard but not its bucket), the folder tree (ancestors sit in other pairs, so workers stage and the parent resolves), the statistics merge (`StatsCollector.merge`), the counts sum, the lock and every tag. `Artifact.assemble` concatenates the parts' already-encoded frames into the real key with `compression=None` – eager, so an empty sweep still truncates a stale artifact, while `assemble_diff` is lazy; the csv header is its own first part (`statement_csv_header`). `index.json` (metadata, registers what the others wrote, so it runs last) is the only artifact left outside the sweep; both are `VersionedArtifact`s, never compressed. Compression is anystore's: one `compression=` on `Store.open`, the funnel every IO surface reaches the backend through, so nothing here sandwiches a codec. An artifact's writer opens eagerly – an export is a whole picture of the store, so an empty sweep must truncate a stale file rather than leave it – while a diff writer is `lazy=True`, since no changes legitimately means no file. Diff ops are `ADD` (`min(first_seen) >= since` – every statement is new), `MOD` (predates the window and changed in it, including a partial delete) and `DEL`; both bounds span *all* statements, `id` rows included, so `to_dict()["first_seen"]` (a non-BASE `min`) is deliberately not reused. Deletes are invisible to a live-view sweep, so the DEL candidates are loaded before it opens and each series claims the ids it meets alive – the remainder is its DEL set. That load is *one* `deleted_candidates_select` pass over `statement_raw` for every active series (`ExportSession.load_pending`), taken at the earliest window any of them needs: `deleted_at` is no partition column, so the pass is the whole store whatever the bound, and asking per series paid for it per series – three passes before the first row of a full export. Rows are filtered on `deleted_at IS NOT NULL` and candidacy is the `HAVING max(deleted_at) >= since`, so the per-entity aggregates (`origins`, `schemata`, `content_hash`) describe the entity rather than just its newest tombstones – a document deleted in two steps would otherwise have lost the `contentHash` row that identifies it. Each series then narrows in python (`DiffableRun.claims`): its own window, and for the document scopes the origin plus the schema half of `Q_DOCUMENTS` (`DocumentsArtifact.is_document_schema`, the one spelling the live path shares). Each series keeps its own diff files, freshness tag and state; entity diffs are not origin-scoped. The first sweep of a series writes no diff file – it records the state the next diff is taken against, the full picture at that point being the export itself
6. **Files**: Source files → Archive (content-addressed by SHA256 checksum)

### Storage Layers

- **JournalStore**: append-only write-ahead log, one keyless/index-free table per dataset carrying `JOURNAL_SCHEMA` (built by `model.statement.journal_table`, `NOT NULL` on `REQUIRED_COLUMNS`) – the parquet columns minus `shard`, so a flush is Arrow in, Arrow out, never a repack. There is deliberately no shard key in the journal: a journalled row routinely outlives the process that wrote it (a postgres journal is shared across runs and hosts), so a stored shard could encode a count that is no longer configured – `ParquetStore.append` derives it at drain time instead. `flush_batches()` rotates the journal (rename to a `journal_{ds}-seg-{ts}` segment + `CREATE TABLE` in one DDL transaction; the rename's exclusive lock drains in-flight writers, and blocked writers re-resolve into the new table), then hands each segment over in whole tables of `LAKEHOUSE_JOURNAL_DRAIN_ROWS` rows (`Settings.journal_drain_rows`, read once per store into `drain_rows`) and `DROP`s it only once the consumer comes back for more – so a failed downstream write keeps its rows, and an abandoned drain leaves an orphan segment the next flush picks up. Segments stream out unordered – there is no `shard` column to sort on, and the `ORDER BY shard` this replaced was an un-indexed pass over the whole segment that had to finish before the first row could be handed over. The whole window is held under `flush_lock()` (postgres session advisory lock, released by the dying connection; an in-process lock on sqlite) – without it a second flush would drain the first one's segment. `flush_batches` / `iterate_entity` are `@no_api`: a journal is drained by the store that holds it, so there is no flush route and a repository in api mode delegates its whole flush to the server (`/entities/flush`). `count` / `iterate_entity` / `clear` span live + segments. Dedup is `merge`'s job: nothing upserts, so `(origin, id, fragment, role)` provenance survives. The dialect is a construction-time pick (`sql_journal` → `PostgresJournalStore` with ADBC Arrow row IO / `SqliteJournalStore`), mirroring how `get_journal` picks `ApiJournalStore` when `Settings.api_mode` is on (global `LAKEHOUSE_URI` starts with `http`) – production API deployments must set the env var. SQL engines use `NullPool` (except in-memory SQLite) so the unbounded `get_journal` cache doesn't accumulate engine connections; the postgres *write* path bypasses the engine and borrows from an ADBC pool keyed on the journal uri and shared process-wide – nothing about an ADBC connection is dataset-scoped, while `get_journal` caches a store per dataset forever, so a pool per store sized idle connections by the dataset count instead of by `LAKEHOUSE_JOURNAL_POOL_SIZE` (`0` to pool nothing) – a cold ADBC connection costs ~60ms and the journal opens one per writer. Checkouts are ping-validated, so a connection the server dropped is retired rather than handed to a writer, and check-in rolls back, so a failed insert's aborted transaction never reaches the next one.
- **ParquetStore**: Delta Lake parquet via ftmq's `LakeStore`, partitioned by `(shard, bucket, origin)`, created with log-bounding table properties (`TABLE_CONFIGURATION`: 1h `remove` retention in checkpoints, 1 day log retention – nothing time-travels; `configure_table` / the `migrate_parquet_table_properties` migration for older stores). `merge` loads one Delta snapshot per run and never `delta_scan`s: each dirty partition's files go to `merge_partition` (in-process, or a spawned `LAKEHOUSE_WORKERS` pool – a worker gets plain data, replays no log, writes files, commits nothing and logs nothing, returning its `took` for the parent to log), which reads them with `read_parquet` and writes one merged file with DuckDB `COPY`; `_commit_merged` commits `add`/`remove` actions in batches of `MERGE_COMMIT_BATCH` via `create_write_transaction` (decoded table-relative paths – the log encodes them), and a run that committed writes a checkpoint. No range slicing: DuckDB bounds memory by spilling, and a partition too large for its worker's share of the limit wants more shards. Reads reconcile: `_cursor_over(source, clean)` builds the per-cursor `statement` view from `live_rows_sql` (plain scan) when every partition in the source is *clean* – all its active files named `merged-*` (`MERGED_PREFIX`), i.e. written by `merge` – and from `dedupe_rows_sql` (`_dedupe_sql`, the same windows merge uses, `entity_id` in every key so lookups push below them) otherwise; `needs_merge` / `merge` select dirty partitions from the same snapshot file list, so there are no per-partition tags. Merge commits carry `dataChange=false`. `_list_partitions` reads the snapshot's partitions, and reads skip `(shard, bucket)` pairs the query's prune excludes (`_prune_values`, ftmq's flat-AND soundness rule). Methods: `append` (derives the `shard` partition key from `entity_id` via `_with_shard` – the one place that happens – then writes, deliberately unsorted; nothing reads in physical order and `merge` rewrites every partition it touched anyway), `merge` (per-partition dedup + tombstone reap), `vacuum` (delete obsolete files). `pa.Table` is the currency end to end – what `statements_to_arrow` packs, what the journal drains, what `append` takes (in `JOURNAL_SCHEMA`, one column short of what it writes).
- **TagStore**: Key-value freshness tracking.
- **VersionStore**: Timestamped snapshots for config / index files.

### Statement currency

`ftm_lakehouse.model.statement.LakehouseStatement` is the canonical statement of the write path – ftmq's `LakeStatement` plus the two columns the lakehouse adds to the schema: `deleted_at` (tombstone marker) and `role` (who asserted the statement, as against `origin`'s where), so nothing has to carry a parallel tuple. It carries no `shard` at all – a statement is content plus provenance, and the partition it lands in is `ParquetStore.append`'s call. It flows out of `EntityBuffer` through `statements_to_arrow` – the one packer shared by the journal writer (one table per insert batch) and the direct bulk path (`flush_table()`, one table per drain), layered on ftmq's columnar `statements_to_table` plus the `deleted_at` column and the two lakehouse fill rules; the journal *drain* builds none at all – it streams Arrow tables into `write_batches`, the one append loop every packed path shares. Statement semantics stay local on purpose: ftmq's lake store partitions by dataset and deletes physically, so sharding and `deleted_at` are not modelled upstream. Statement ids are content-hashed under the *target* dataset at the buffer boundary: `EntityBuffer.add_statement` re-keys every incoming statement (ignoring carried-over ids) and `add_entity` re-derives the FtM BASE checksum over the re-keyed ids – so identical content collapses on merge regardless of the payload's `datasets` context or CSV round-trips; the unsafe explode path mirrors this exactly. `LakehouseStatement` inherits `fragment` from `LakeStatement`; the empty string is the "no fragment" sentinel everywhere – storage never holds NULL fragments. `role` is the opposite convention on purpose: nullable, outside `REQUIRED_COLUMNS`, NULL is "no role" (the `deleted_at` shape) and the empty string collapses to NULL so there is one representation. It is still row identity – `dedupe_key` is `(id, origin, fragment, role)` and `merge` keys both branches on it – which is sound with NULLs because DuckDB groups them together in a window `PARTITION BY`. `role` is *not* part of the content hash: identical content keeps one statement id whoever asserts it, so two roles produce two rows sharing an id. `read_csv_statements` lives in `model/statement.py` rather than `helpers/` because it has to build a `LakehouseStatement`, which `helpers/` may not import. The `fragment` column, its writer properties, and `LakeStatement` live upstream in ftmq (`ftmq.store.lake`); the supersession semantics (two-branch dedupe view + merge) stay in the lakehouse. The journal has no key at all – `fragment` is just one of its `JOURNAL_SCHEMA` columns, as in parquet.

### Tag-based Freshness

Operations use tags to track freshness and skip unnecessary work:

- `statements/last_updated` – the store's content moved: rows landed (a flush / append) or an origin was dropped. The one clock exports, statistics and diffs depend on; a merge rewrites files, not content, and leaves it alone
- `operations/optimize/last_run` – stamped for the record by `optimize`, which decides freshness from the store's dirty partitions (`needs_merge`), not from a tag
- `archive/last_updated` – File was archived
- Export targets double as their freshness tags (`exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv`, `exports/documents.{origin}.csv`, `exports/statistics.json`, `index.json`). The run targets `operations/export/last_run` and stamps every artifact tag once the sweep has returned – so a crash part-way stamps nothing. They are the published record of when each artifact was written, and `DownloadArchiveOperation` keys its own freshness on one of them (`exports/documents.csv`)

### Sharding

Each dataset's parquet store is partitioned by `(shard, bucket, origin)`:

- `shard` = `hash(entity_id) % shards`, hex-padded. Per-dataset configuration (`shards` in `config.yml`, hardcoded default `0`; `shards <= 1` means a single shard `"0"`); there is deliberately no env override – every reader/writer resolves the count from the dataset's config (`DatasetHandle._model`). Derived in `ParquetStore.append` (`_with_shard`, hashing the *distinct* `entity_id`s via `dictionary_encode`) and nowhere else: every producer – journal drain, `EntityBuffer.flush_table`, the unsafe `RowBuffer`, the api bulk route – hands over rows with no shard key, so a partition is always picked against the count the writing store is configured for, never one a producer resolved earlier or elsewhere. Huge datasets should configure `8`+ at creation (`ensure_dataset(name, shards=8)`) – see `docs/architecture.md`. Fixed once the store is written; `ShardOperation` (`maintenance shard --shards n`) is the rewrite that changes it – one streamed `write_deltalake` per `(bucket, origin)` group (bucket/origin are invariant, only `shard` moves), config written last, no dedupe or sort so every partition comes out dirty for the next merge. Journal writes aren't fenced – run it with writers stopped: rows journalled during the rewrite carry no shard key, but a flush landing between the rewrite and the config write still resolves the old count.
- `bucket` = coarse FtM schema group (`thing`, `interval`, `document`, `page`, `pages`, `mention`).
- `origin` = caller-supplied source tag.

### ZFS Integration

When deployed on ZFS (`LAKEHOUSE_ON_ZFS=1`), the lakehouse auto-creates ZFS datasets with tuned properties per storage type. The transport (local subprocess vs. socket agent, chown, peer auth) is the external `zfs-agent` package (github.com/dataresearchcenter/zfs-agent, its own `ZFS_*` env + `zfs-agent` host command); the lakehouse only owns the tuning and the caller.

- **`core/zfs.py`**: `DatasetConfig` tuning + `ensure_zfs_dataset(pool, dataset)` calling `zfs_agent.zfs_create`. `archive` uses `zstd-9`; `statements` uses `compression=off` because parquet handles compression internally.
- **`cli/zfs.py`**: `ftm-lakehouse zfs init` (manual dataset creation). The agent daemon is the package's own `zfs-agent` command.
- **Settings**: `LAKEHOUSE_ON_ZFS`, `LAKEHOUSE_ZFS_POOL` only – socket/owner/peer-auth are the package's `ZFS_*` env.

### Configuration

Settings via environment variables with `LAKEHOUSE_` prefix:

- `LAKEHOUSE_URI`: Base storage path (default: `data`)
- `LAKEHOUSE_JOURNAL_URI`: Journal database URI (default: `sqlite:///:memory:`)
- `LAKEHOUSE_API_KEY` / `LAKEHOUSE_API_SECRET`: client-side auth headers attached to outgoing lakehouse-API requests (`core/api.py`); authenticate through the nginx proxy in the docker stack
- `LAKEHOUSE_GRACE_PERIOD_DAYS`: Tombstone grace period for `merge` (default: `30`)
- `LAKEHOUSE_MAX_BUFFER_ROWS`: Row cap on `EntityBuffer` before a flush is required; bulk-import paths raise `BufferFullError` past this point (default: `1_000_000`)
- `LAKEHOUSE_JOURNAL_DRAIN_ROWS`: Rows per Arrow table a journal flush hands to the parquet store. One table becomes one parquet file per `(shard, bucket, origin)` it spans (plus a Delta commit per bucket), so this is the knob for how big ingest's files are – with 256 shards the default is ~4k rows a file. Bounded by memory: the table is held whole while it is written (default: `1_000_000`)
- `LAKEHOUSE_JOURNAL_POOL_SIZE`: Postgres journal connections kept warm between writers – one pool per journal uri per process, whatever the dataset count; `0` pools nothing. Bounds idle connections only – writers beyond it open their own rather than queueing (default: `5`)
- `LAKEHOUSE_LOCK_MAX_RETRIES`: Retry bound for every lock wait (`.LOCK` / `.LOCK-MERGE` acquisition, appends waiting out a held `.LOCK`); total wait ≈ N²/2 seconds, then `RuntimeError`. Stale locks need `maintenance unlock` (default: `22`)
- `LAKEHOUSE_DUCKDB_MEMORY_LIMIT`: Per-DuckDB-connection RAM ceiling; queries beyond it spill to disk (default: `8GB`)
- `LAKEHOUSE_WORKERS`: Processes the parallel maintenance paths fan partitions out to – `merge` and the export sweep share the knob; `1` runs in-process (the builtin `map`, exactly the serial path). The DuckDB memory limit and threads are split between them, so the limit stays the ceiling for the whole operation. A sweep is bounded by the `(shard, bucket)` pair count (default: `1`)
- `LAKEHOUSE_DUCKDB_TEMP_DIRECTORY`: Spill-to-disk path for DuckDB – chiefly the sweep's per-partition sort (default: `{OS temp dir}/duckdb`; DuckDB's own default is `.tmp` relative to the working directory, so this is set explicitly. Empty falls back to that)
- `LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY`: Where DuckDB loads/auto-installs extensions; unset = `$HOME/.duckdb/extensions` (breaks without a writable `HOME` – the Docker image pre-installs `delta` into `/opt/duckdb/extensions` and sets this)
- `LAKEHOUSE_ON_ZFS`: Enable ZFS dataset creation (default: `false`)
- `LAKEHOUSE_ZFS_POOL`: ZFS pool path for dataset creation

API settings use `LAKEHOUSE_API_` prefix:

- `LAKEHOUSE_API_QUERY_MAX_IN_VALUES`: Max values per `in`/`not_in` filter in a query body (default: `10_000`)
- `LAKEHOUSE_API_QUERY_MAX_FILTER_KEYS`: Max filter leaves in a query body (default: `20`)

Full operator-facing list with explanations: `docs/deployment/configuration.md`.

### CLI

Main CLI entry point: `ftm-lakehouse` (typer-based)
- Uses `-d` flag for dataset name in most commands
- Sub-typer groups: `maintenance` (`flush` / `optimize` / `shard` / `migrate` / `unlock`, + top-level `configure` / `make` / `export` / `crawl` shortcuts), `entities`, `statements`, `archive`, `zfs`
- `configure -c <yml>` writes config only; `make -c` runs the same `write_config` helper first. Both merge (`exclude_unset=True`), so a partial yaml doesn't reset `shards` to the default
- `make` runs flush → optimize (merge + vacuum) → export, all on by default (`--no-flush` / `--no-exports` / `--no-optimize`, plus `--force-optimize` / `--force-exports`); `--no-exports` flushes only. `MakeOperation` itself only flushes and exports – reads reconcile un-merged rows – so the optimize is an optimisation the CLI runs first
- `DatasetContext` yields `(name, uri)` and ensures the dataset on entry; commands resolve repos via the factories
- Shared command options are `OPT_*` `Annotated` constants in `cli/__init__.py`; new sub-typer groups go through `sub_typer(name, help)`
- `statements sql` and `maintenance unlock` are local-only – they raise `RuntimeError` in api mode (raw SQL / lock-file manipulation deliberately have no api wire)
- `SKIP_CATALOG_COMMANDS = {"zfs"}` in `cli/__init__.py` bypasses catalog loading for commands that don't need it

## Testing

- **Claude: only run the tests for the parts you touched** (specific files / `-k` selections). Running the full suite and linting is handled by the user.
- Tests organized by type: `test_unit_*.py`, `test_integration_*.py`, `test_e2e_*.py`
- Test fixtures in `tests/fixtures/`
- Uses moto for S3 mocking, RangeHTTPServer for HTTP fixtures
- Fixtures auto-clear factory caches between tests to prevent cross-test pollution
- Repository / e2e fixtures are parametrized over `local` / `api` / `docker` variants. The `docker` variants run against the compose stack (`make start`) through nginx with api-key auth and only execute when `LAKEHOUSE_TEST_MODE=docker`; otherwise they auto-skip. `make test` orchestrates both passes. Docker tests use unique `e2e_<hex>` dataset names and can assert on-disk layout via the `./data` bind mount.
- The postgres journal (`PostgresJournalStore`, ADBC Arrow row IO) is only exercised when `PYTEST_POSTGRESQL_URI` points at a live server – its fixture variants auto-skip otherwise, so sqlite is what a plain run covers. Both drivers come from the `postgres` extra (`poetry install --all-extras`).
- pytest env defaults live in `[tool.pytest_env]` in `pyproject.toml` – TOML dict form (`KEY = {value = "x", skip_if_set = true}`), not the INI `D:` prefix

## Code Style

- Formatting: black, isort (profile=black)
- Pre-commit hooks enforce style
- Uses absolute imports (absolufy-imports)
- All imports at the top of the file, sorted by isort. Inline imports (inside functions / methods) are only acceptable when needed to break a circular import; if you reach for one, leave a comment explaining the cycle.
- `mypy --strict` carries a pre-existing error baseline (generics / dependency-bump mismatches across ~50 files); when changing code, check that your touched files add no new errors instead of trying to fix the baseline.
