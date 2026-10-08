# REST API

`ftm-lakehouse` ships a FastAPI app (the `api` extra) exposing blob storage, the journal, entity / statement reads and writes, and dataset job execution over HTTP. It has **no authentication, authorization or rate limiting** – those belong in front of it.

!!! info "Use a reverse proxy in production"

    ### File serving

    The api serves blobs (`HEAD` / `GET`), but production should serve files from a static file server like nginx – see [PutFS](https://putf.sh).

    ### Authentication

    Run the API behind a reverse proxy (Caddy / nginx / Traefik / a sidecar) that handles authentication, authorization and rate limiting. The [PutFS auth model](https://putf.sh/reference/auth/) shows how to scope tokens by path prefix and HTTP method at the proxy.

    ### Request timeouts

    The API enforces no per-request timeout. Configure ``proxy_read_timeout`` (nginx), ``timeouts`` (Caddy) or the equivalent in your proxy.

    ### Request body size

    The API does not cap request body size. Configure ``client_max_body_size`` (nginx), ``request_body`` (Caddy) or the equivalent. Query bodies are still validated against the [limits below](#configuration).

## Running the API

```bash
granian --interface asgi ftm_lakehouse.api:app
```

The interactive API docs (ReDoc) are served at `/`.

## Routes

Lakehouse routes are scoped to a dataset under `/{dataset}/_api/...`. Blobs are served by the [PutFS](https://putf.sh) app mounted at `/` behind them; the api only serves a local-path lakehouse.

### Journal

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/journal/bulk` | Write statement rows into the journal |

The body is an Arrow IPC stream (`application/vnd.apache.arrow.stream`) of the statement schema. There is no flush route here: a repository in api mode flushes through `/entities/flush`.

### Entities

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/entities/flush` | Drain the journal into parquet |
| `POST` | `/{dataset}/_api/entities/query` | Query entities, streamed as NDJSON |
| `POST` | `/{dataset}/_api/entities/statements/query` | Query raw statements, streamed as NDJSON |
| `GET` | `/{dataset}/_api/entities/stats` | Dataset statistics |
| `GET` | `/{dataset}/_api/entities/statements/version` | Current Delta table version |
| `DELETE` | `/{dataset}/_api/entities/{entity_id}` | Tombstone all statements of an entity (`?origin=` narrows to one origin) |
| `DELETE` | `/{dataset}/_api/entities/origins/{origin}` | Physically drop an origin's partitions |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_api/operations` | Run a job operation on a dataset |
| `POST` | `/{dataset}/_api/ensure` | Create the dataset's ZFS datasets when ZFS is configured (like every write) – call it before the first action on a dataset |

The operations body is a serialized `DatasetJobModel` with a `name` field identifying the operation:

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
| [`OptimizeJob`](../reference/operation.md#ftm_lakehouse.operation.maintenance.OptimizeJob) | Merge dirty partitions (dedupe, reap tombstones), then vacuum the replaced files |
| [`ExportJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportJob) | Export every artifact from one sweep, then `index.json`. `make_diff` (default `true`) also writes the delta diff files |
| [`DownloadArchiveJob`](../reference/operation.md#ftm_lakehouse.operation.download.DownloadArchiveJob) | Export archive files to original paths |
| [`MakeJob`](../reference/operation.md#ftm_lakehouse.operation.make.MakeJob) | Flush the journal, then export (no merge) |

Pass `?force=true` to skip freshness checks.

## Configuration

API-only settings use the `LAKEHOUSE_API_` prefix:

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_API_TITLE` | OpenAPI title | `FollowTheMoney Data Lakehouse Api` |
| `LAKEHOUSE_API_DESCRIPTION` | OpenAPI description | contents of `./README.md` |
| `LAKEHOUSE_API_CONTACT__NAME` / `__URL` / `__EMAIL` | OpenAPI contact | (unset) |
| `LAKEHOUSE_API_QUERY_MAX_IN_VALUES` | Maximum values in one `in` / `not_in` filter of a query body | `10_000` |
| `LAKEHOUSE_API_QUERY_MAX_FILTER_KEYS` | Maximum filter leaves in a query body | `20` |

Storage URI, journal URI and the rest use the regular `LAKEHOUSE_` settings – see [Configuration](configuration.md).
