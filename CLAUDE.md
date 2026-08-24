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
    Sphinx-style refs: :class:`LakehouseStatement`, :meth:`flush`.

    Args:
        grace_period_days: Override ``settings.grace_period_days``. Pass ``0``
            to drop tombstones immediately.

    Returns:
        Number of statements merged.

    Yields:
        (only for generators) ``LakehouseStatement`` sorted by shard.

    Raises:
        RuntimeError: when the dataset write fence cannot be acquired.
    """
```

- Use ``Args``, ``Returns`` / ``Yields`` (only one, matching the function), ``Raises`` – in that order.
- Indent the body of each section by 4 spaces under the section header.
- Backtick code/identifiers in prose (``` ``foo`` ```). Use Sphinx ``:class:`` / ``:meth:`` / ``:func:`` refs for in-package cross-links.

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
│   │                    #   + top-level `make` / `export <kind>` shortcuts
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

1. **Writing (journal-backed)**: Entities → `EntityBuffer` → SQL Journal (same columns as parquet) → (via `flush`, as Arrow tables) → Parquet Store
2. **Writing (direct bulk)**: Entities → `EntityBuffer` (in-memory, shard-grouped) → `buffer.flush_tables()` (one packed table per shard) → `repo.write_batches` → Parquet Store. With `--unsafe` on the CLI import commands: payload dicts → `logic/entities/explode.py` (packed row dicts, no FtM object construction; parity-tested against the safe path incl. statement ids and namespace stripping) → `RowBuffer` (flushes a `SHARDED_SCHEMA` table) → `repo.write_batches` → Parquet Store
3. **Querying**: Parquet Store via ftmq's `LakeStore` (DuckDB `delta_scan`) with two registered views – deduped `statement` (every read, including stats) and `statement_raw` (tombstones + physical layout visible; used by `merge` and diff exports). The deduped view (and `merge`) routes rows into two isolated branches on `fragment`: non-fragment rows (`fragment = ''`) dedupe per statement `id`, fragment rows supersede per `(origin, entity_id, prop, fragment)` group (latest emission wins, ties survive together – see `docs/usage/entities.md#fragment-supersession`). Statement reads iterate `(shard, bucket)` partitions in Python so predicate pushdown keeps the dedupe window bounded per partition. Filters reach SQL by compiling an ftmq `Query` (node DSL – `M` meta / `P` property / `G` group / `C` context, not flat kwargs) through a lakehouse `SqlSource` (`make_source` in `storage/parquet.py`; one for the live view, one for `statement_raw`) – keyed on `entity_id` (no physical `canonical_id`) with two partition prunes folded into every clause: schema→`bucket` and `entity_id`→`shard` (`make_prune_by_shard`). `origin` is an ordinary `Query` node like any other filter. The HTTP query endpoints carry the whole query as a JSON dict (`Query.to_dict` / `from_dict`), with `flush_first` as the only sibling body field.
4. **Maintenance (async)**: reads downstream of the store (exports, statistics, diffs) require a **merged** store – the live view does no read-time dedupe. `DatasetJobOperation.prepare()` runs before the freshness window opens (`Tags.touch` stamps the target with its *entry* time, so preparing inside the window would backdate the result); `ExportOperation.prepare` flushes and merges there, so exports self-heal and their timestamps stay honest. `OptimizeOperation` overrides `is_fresh()` to ask `ParquetStore.needs_merge` (the per-partition tags `merge` compares) instead of the dataset tag pair, which is unsound: the target records the operation's start while `merge` stamps on completion. One `optimize` operation runs the three storage primitives in order – `merge` (per-partition dedup + tombstone reap with grace), `compact` (file bin-pack), `vacuum` (drop obsolete files); `ParquetStore.shard` is the fourth, moving rows *between* partitions. The write fence is split: maintenance takes the exclusive `.LOCK` and drains in-flight append markers (`.LOCK-APPENDS/`); appends register a marker *first*, then back off (marker removed) while `.LOCK` is held – the store-then-load order is what makes the drain sound – and run concurrently, Delta's optimistic concurrency serializing their commits. Table creation is one empty commit under the exclusive lock (`ParquetStore._ensure_table`). All waits bounded by `LAKEHOUSE_LOCK_MAX_RETRIES`; stale locks/markers need `ftm-lakehouse maintenance unlock`. Exports and stats assume an optimized store.
5. **Exporting**: Parquet Store → `statements.csv` / `entities.ftm.json` / `statistics.json` / `documents.csv` / `index.json`
6. **Files**: Source files → Archive (content-addressed by SHA256 checksum)

### Storage Layers

- **JournalStore**: append-only write-ahead log, one keyless/index-free table per dataset carrying `SHARDED_SCHEMA` (built by `model.statement.journal_table`, `NOT NULL` on `REQUIRED_COLUMNS`) – so a flush is Arrow in, Arrow out, never a repack. `flush_batches(ordered)` rotates the journal (rename to a `journal_{ds}-seg-{ts}` segment + `CREATE TABLE` in one DDL transaction; the rename's exclusive lock drains in-flight writers, and blocked writers re-resolve into the new table), then hands each segment over in whole `DRAIN_BATCH_SIZE` tables and `DROP`s it only once the consumer comes back for more – so a failed downstream write keeps its rows, and an abandoned drain leaves an orphan segment the next flush picks up. The whole window is held under `flush_lock()` (postgres session advisory lock, released by the dying connection; an in-process lock on sqlite) – without it a second flush would drain the first one's segment. `flush_batches` / `iterate_entity` are `@no_api`: a journal is drained by the store that holds it, so there is no flush route and a repository in api mode delegates its whole flush to the server (`/entities/flush`). `count` / `iterate_entity` / `clear` span live + segments. Dedup is `merge`'s job: nothing upserts, so `(origin, id, fragment)` provenance survives. The dialect is a construction-time pick (`sql_journal` → `PostgresJournalStore` with ADBC Arrow row IO / `SqliteJournalStore`), mirroring how `get_journal` picks `ApiJournalStore` when `Settings.api_mode` is on (global `LAKEHOUSE_URI` starts with `http`) – production API deployments must set the env var. SQL engines use `NullPool` (except in-memory SQLite) so the unbounded `get_journal` cache doesn't accumulate engine connections; the postgres *write* path bypasses the engine and borrows from its own ADBC pool (`LAKEHOUSE_JOURNAL_POOL_SIZE` idle connections per cached dataset, `0` to pool nothing) – a cold ADBC connection costs ~60ms and the journal opens one per writer. Checkouts are ping-validated, so a connection the server dropped is retired rather than handed to a writer, and check-in rolls back, so a failed insert's aborted transaction never reaches the next one.
- **ParquetStore**: Delta Lake parquet via ftmq's `LakeStore`, partitioned by `(shard, bucket, origin)`. Methods: `append` (per-shard write, deliberately unsorted – nothing reads in physical order and `merge` rewrites every partition it touched anyway), `merge` (per-partition dedup + tombstone reap), `compact` (file bin-pack), `vacuum` (delete obsolete files). `pa.Table` is the currency end to end – what `statements_to_arrow` packs, what the journal drains, what `append` takes.
- **TagStore**: Key-value freshness tracking.
- **VersionStore**: Timestamped snapshots for config / index files.

### Statement currency

`ftm_lakehouse.model.statement.LakehouseStatement` is the canonical statement of the write path – ftmq's `LakeStatement` plus the two columns the lakehouse adds to the schema, `shard` and `deleted_at`, so nothing has to carry a parallel tuple. It flows out of `EntityBuffer` through `statements_to_arrow` – the one packer shared by the journal writer (`flush_buffer()`, one table per insert batch) and the direct bulk path (`flush_tables()`, one table per shard), layered on ftmq's columnar `statements_to_table` plus the `shard` / `deleted_at` columns and the two lakehouse fill rules; the journal *drain* builds none at all – it streams Arrow tables into `write_batches`, the one append loop every packed path shares. Statement semantics stay local on purpose: ftmq's lake store partitions by dataset and deletes physically, so `shard` and `deleted_at` are not modelled upstream. Statement ids are content-hashed under the *target* dataset at the buffer boundary: `EntityBuffer.add_statement` re-keys every incoming statement (ignoring carried-over ids) and `add_entity` re-derives the FtM BASE checksum over the re-keyed ids – so identical content collapses on merge regardless of the payload's `datasets` context or CSV round-trips; the unsafe explode path mirrors this exactly. `LakehouseStatement` inherits `fragment` from `LakeStatement`; the empty string is the "no fragment" sentinel everywhere – storage never holds NULL fragments. The `fragment` column, its writer properties, and `LakeStatement` live upstream in ftmq (`ftmq.store.lake`); the supersession semantics (two-branch dedupe view + merge) stay in the lakehouse. The journal has no key at all – `fragment` is just one of its `SHARDED_SCHEMA` columns, as in parquet.

### Tag-based Freshness

Operations use tags to track freshness and skip unnecessary work:

- `journal/last_updated` – Journal has uncommitted data
- `journal/last_flushed` – Journal was flushed to parquet
- `statements/last_updated` – rows landed in the parquet store (appends); the append-side clock, not canonical yet
- `statements/last_optimized` – stamped by `merge` on completion when it rewrote something: the canonical-content clock every export / statistic / diff depends on
- `archive/last_updated` – File was archived
- Export targets double as their freshness tags (`exports/statements.csv`, `entities.ftm.json`, `exports/documents.csv`, `exports/statistics.json`, `index.json`)

### Sharding

Each dataset's parquet store is partitioned by `(shard, bucket, origin)`:

- `shard` = `hash(entity_id) % shards`, hex-padded. Per-dataset configuration (`shards` in `config.yml`, hardcoded default `0`; `shards <= 1` means a single shard `"0"`); there is deliberately no env override – every reader/writer resolves the count from the dataset's config (`DatasetHandle._model`). Huge datasets should configure `8`+ at creation (`ensure_dataset(name, shards=8)`) – see `docs/architecture.md`. Fixed once the store is written; `ShardOperation` (`maintenance shard --shards n`) is the rewrite that changes it – one streamed `write_deltalake` per `(bucket, origin)` group (bucket/origin are invariant, only `shard` moves), config written last, no dedupe or sort so every partition comes out dirty for the next merge. Journal writes aren't fenced – run it with writers stopped.
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
- `LAKEHOUSE_JOURNAL_POOL_SIZE`: Postgres journal connections kept warm between writers, per *cached dataset journal*; `0` pools nothing. Bounds idle connections only – writers beyond it open their own rather than queueing (default: `5`)
- `LAKEHOUSE_LOCK_MAX_RETRIES`: Retry bound for every write-fence wait (exclusive `.LOCK` acquisition, appends waiting out a held `.LOCK`, maintenance draining `.LOCK-APPENDS/` markers); total wait ≈ N²/2 seconds, then `RuntimeError`. Stale locks/markers need `maintenance unlock` (default: `22`)
- `LAKEHOUSE_DUCKDB_MEMORY_LIMIT`: Per-DuckDB-connection RAM ceiling; queries beyond it spill to disk (default: `8GB`)
- `LAKEHOUSE_DUCKDB_TEMP_DIRECTORY`: Spill-to-disk path for DuckDB; unset = OS temp dir
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
- Sub-typer groups: `maintenance` (`flush` / `optimize` / `shard` / `unlock`, + top-level `configure` / `make` / `export` / `crawl` shortcuts), `entities`, `statements`, `archive`, `zfs`
- `configure -c <yml>` writes config only; `make -c` runs the same `write_config` helper first. Both merge (`exclude_unset=True`), so a partial yaml doesn't reset `shards` to the default
- `make` runs flush → optimize → exports, all on by default (`--no-flush` / `--no-exports` / `--no-optimize`, plus `--force-optimize` / `--force-exports`)
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
