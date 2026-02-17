# REST API

`ftm-lakehouse` includes a FastAPI-based REST API for remote access to the lakehouse. It exposes journal operations and dataset job execution over HTTP with JWT-based authentication.

## Running the API

```bash
uvicorn ftm_lakehouse.api:app --reload  # disable --reload for production
```

The interactive API docs (ReDoc) are served at `/`.

## Configuration

API settings use the `LAKEHOUSE_API_` prefix:

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_API_SECRET_KEY` | JWT signing key | `change-for-production` |
| `LAKEHOUSE_API_ACCESS_TOKEN_EXPIRE` | Token expiry in minutes | `5` |
| `LAKEHOUSE_API_ACCESS_TOKEN_ALGORITHM` | JWT algorithm | `HS256` |
| `LAKEHOUSE_API_AUTH_REQUIRED` | Require authentication | `true` |
| `LAKEHOUSE_API_TITLE` | OpenAPI title | `FollowTheMoney Data Lakehouse Api` |

When `auth_required` is `false`, read-only requests (`GET`, `HEAD`, `OPTIONS`) are allowed without a token. Write requests are always rejected in public mode.

## Authentication

The API uses JWT bearer tokens with a method + path prefix authorization model. Tokens encode a list of allowed HTTP methods and path prefixes, keeping auth logic external to the API itself.

### Token structure

Tokens carry two claims:

- **methods**: List of allowed HTTP methods (e.g. `["GET", "POST"]`) or `["*"]` for all
- **prefixes**: List of allowed path prefixes or glob patterns

### Examples

Allow all access:

```python
from ftm_lakehouse.api.auth import create_access_token

token = create_access_token(methods=["*"], prefixes=["/"])
```

Read-only access:

```python
token = create_access_token(methods=["GET", "HEAD", "OPTIONS"], prefixes=["/"])
```

Scoped to a dataset's archive:

```python
token = create_access_token(methods=["*"], prefixes=["/my_dataset/archive/"])
```

Glob pattern matching:

```python
token = create_access_token(methods=["*"], prefixes=["/*/tags"])
```

## Routes

### Storage

The base storage routes are provided by [anystore](https://docs.investigraph.dev/lib/anystore/) and expose raw key-value access to the underlying lakehouse store. All keys are path-based -- for example, `my_dataset/archive/ab/cd/ef/{checksum}/blob` addresses a file blob.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/{key:path}` | Retrieve a stored value by key |
| `GET` | `/{prefix:path}?keys=true` | List all keys under a prefix |
| `GET` | `/{prefix:path}?keys=true&glob=*.json` | List keys matching a glob pattern |
| `HEAD` | `/{key:path}` | Get metadata (size, content type, timestamps) |
| `HEAD` | `/{key:path}?checksum=true` | Get metadata with checksum in `x-anystore-checksum` header |
| `PUT` | `/{key:path}` | Store a value (request body streamed directly to storage) |
| `DELETE` | `/{key:path}` | Delete a value |
| `PATCH` | `/{key:path}` | Touch a key (update its timestamp) |

`GET` supports HTTP range requests via the `Range` header (e.g. `Range: bytes=0-1023`) and returns `206 Partial Content` with the requested byte range.

Response headers include `Content-Length`, `Content-Type`, `Accept-Ranges`, `Last-Modified`, and `x-anystore-*` metadata fields.

### Journal

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/journal/bulk` | Write TSV rows into the journal |
| `GET` | `/{dataset}/journal/iterate` | Stream all journal rows as TSV |
| `POST` | `/{dataset}/journal/flush` | Stream and delete journal rows |
| `GET` | `/{dataset}/journal/count` | Get journal row count |
| `DELETE` | `/{dataset}/journal/clear` | Delete all journal rows |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/{dataset}/_operation` | Run a job operation on a dataset |

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
| [`OptimizeJob`](../reference/operation.md#ftm_lakehouse.operation.optimize.OptimizeJob) | Compact parquet files, optional vacuum and translog apply |
| [`ExportStatementsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportStatementsJob) | Export to `statements.csv` |
| [`ExportEntitiesJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportEntitiesJob) | Export to `entities.ftm.json` |
| [`ExportStatisticsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportStatisticsJob) | Export to `statistics.json` |
| [`ExportDocumentsJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportDocumentsJob) | Export to `documents.csv` |
| [`ExportIndexJob`](../reference/operation.md#ftm_lakehouse.operation.export.ExportIndexJob) | Export `index.json` with resources |
| [`MappingJob`](../reference/operation.md#ftm_lakehouse.operation.mapping.MappingJob) | Process a CSV mapping configuration |
| [`RecreateJob`](../reference/operation.md#ftm_lakehouse.operation.recreate.RecreateJob) | Rebuild parquet store from exports |
| [`DownloadArchiveJob`](../reference/operation.md#ftm_lakehouse.operation.download.DownloadArchiveJob) | Export archive files to original paths |
| [`MakeJob`](../reference/operation.md#ftm_lakehouse.operation.make.MakeJob) | Full workflow: flush + all exports |

Pass `?force=true` to skip freshness checks.
