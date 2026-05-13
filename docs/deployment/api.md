# REST API

`ftm-lakehouse` ships a FastAPI app that exposes the storage layer, the journal, the entity / statement read+write paths, and dataset job execution over HTTP. It carries **no authentication, authorization, or rate-limiting logic** – those are deployment concerns and belong in front of the app, not inside it.

!!! info "Use reverse proxy in production"

    ### File serving

    Although the api exposes `HEAD` / `GET` endpoints, for production use it is recommended to use a static file server like nginx. One approach for that is currently researched and developed in [PutFS](https://putf.sh).

    ### Authentication

    The API is intentionally unprotected at the application layer. Run it behind a reverse proxy (Caddy / nginx / Traefik / a sidecar service) that handles authentication, authorization, and rate-limiting before forwarding to the lakehouse.

    [PutFS auth model](https://putf.sh/reference/auth/) is a good reference for how an operator can wire path-prefix + HTTP-method scoped tokens at the proxy layer.

    ### Request timeouts

    The API does not enforce a per-request wall-clock timeout. Configure ``proxy_read_timeout`` (nginx), ``timeouts`` (Caddy), or the equivalent in your proxy to bound how long a request can occupy a connection.

    ### Request body size

    The API does not cap request body size. Configure ``client_max_body_size`` (nginx), ``request_body`` (Caddy), or the equivalent in your proxy. Endpoints that semantically constrain content shape (e.g. ``entities/query`` capping ``entity_ids`` length) still validate after the body is parsed.

## Running the API

```bash
uvicorn ftm_lakehouse.api:app --reload  # disable --reload for production
```

The interactive API docs (ReDoc) are served at `/`.

## Routes

All lakehouse-specific routes are scoped to a dataset and namespaced under `/{dataset}/_api/...`. The raw key-value storage layer (anystore's catch-all `GET /{key:path}` etc.) is mounted last for blob access but is out of scope for this API surface – see the [anystore docs](https://docs.investigraph.dev/lib/anystore/) for that contract.

### Journal

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/journal/bulk` | Write JSONL rows into the journal |
| `GET` | `/{dataset}/_api/journal/iterate` | Stream all journal rows as JSONL |
| `POST` | `/{dataset}/_api/journal/flush` | Stream and delete journal rows as JSONL |
| `GET` | `/{dataset}/_api/journal/count` | Get journal row count |
| `DELETE` | `/{dataset}/_api/journal/clear` | Delete all journal rows |

### Entities

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/entities/flush` | Drain the journal into parquet |
| `POST` | `/{dataset}/_api/entities/merge` | Collapse duplicates + reap expired tombstones |
| `POST` | `/{dataset}/_api/entities/query` | Query entities, streamed as NDJSON |
| `POST` | `/{dataset}/_api/entities/statements/query` | Query raw statements, streamed as NDJSON |
| `GET` | `/{dataset}/_api/entities/stats` | Dataset statistics |
| `GET` | `/{dataset}/_api/entities/statements/version` | Current Delta table version |
| `DELETE` | `/{dataset}/_api/entities/{entity_id}` | Tombstone all statements for an entity |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/operations` | Run a job operation on a dataset |

The request body must be a serialized `DatasetJobModel` with a `name` field identifying the operation:

```json
{
    "name": "CrawlJob",
    "source": "s3://bucket/path"
}
```

Available operations:

| Job name | Description |
|----------|-------------|
| [`CrawlJob`](../reference/operation.md#ftm_lakehouse.operation.crawl.CrawlJob) | Batch file ingestion from a source URI |
| [`CompactJob`](../reference/operation.md#ftm_lakehouse.operation.maintenance.CompactJob) | Bin-pack small parquet files |
| [`MergeJob`](../reference/operation.md#ftm_lakehouse.operation.maintenance.MergeJob) | Per-partition dedup + tombstone reap |
| [`VacuumJob`](../reference/operation.md#ftm_lakehouse.operation.maintenance.VacuumJob) | Delete obsolete parquet files |
| [`ExportStatementsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportStatementsJob) | Export to `statements.csv` |
| [`ExportEntitiesJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportEntitiesJob) | Export to `entities.ftm.json` |
| [`ExportStatisticsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportStatisticsJob) | Export to `statistics.json` |
| [`ExportDocumentsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportDocumentsJob) | Export to `documents.csv` |
| [`ExportIndexJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportIndexJob) | Export `index.json` with resources |
| [`MappingJob`](../reference/operation.md#ftm_lakehouse.operation.mapping.MappingJob) | Process a CSV mapping configuration |
| [`DownloadArchiveJob`](../reference/operation.md#ftm_lakehouse.operation.download.DownloadArchiveJob) | Export archive files to original paths |
| [`MakeJob`](../reference/operation.md#ftm_lakehouse.operation.make.MakeJob) | Full workflow: flush + all exports |

Pass `?force=true` to skip freshness checks.

## Configuration

API-only settings use the `LAKEHOUSE_API_` prefix:

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_API_TITLE` | OpenAPI title | `FollowTheMoney Data Lakehouse Api` |
| `LAKEHOUSE_API_ALLOWED_ORIGINS` | CORS allow-list | `["http://localhost:3000"]` |
| `LAKEHOUSE_API_STATIC_HEADERS` | Extra headers added to every response | `{}` |

Storage URI, journal URI, shard count, etc. use the regular `LAKEHOUSE_` settings – see [Configuration](configuration.md).
