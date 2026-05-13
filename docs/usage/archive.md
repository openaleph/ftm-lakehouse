# Working with Files

The archive repository manages source documents and files metadata in `ftm-lakehouse`. It provides content-addressable storage with automatic deduplication.

## Overview

The archive stores files using their SHA256 checksum as the key. This design enables:

- **Deduplication**: Identical files are stored only once (per dataset)
- **Integrity**: Verify file contents via checksum
- **Metadata**: Track file properties (name, size, MIME type, etc.)
- **Provenance**: Link files to [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entities


!!! info "Blob vs. File object"
    When referring to a _Blob_, this is the actual bytes content of a given source file, identified by it's SHA256 checksum.

    When referring to a _File_, this is the metadata [File](../reference/model.md#ftm_lakehouse.model.file.File) model. Multiple metadata files can exist for a single bytes blob.


## Quick Start

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Archive a source file, returns File metadata:
file = dataset.archive.store("/path/to/document.pdf")
print(f"Archived: {file.name} ({file.checksum})")

# Archive from HTTP URL
file = dataset.archive.store("https://example.com/report.pdf")
print(f"Archived: {file.name} ({file.checksum})")

# Retrieve file content
with dataset.archive.open(file.checksum) as fh:
    content = fh.read()

# Stream bytes (memory efficient for large files)
for chunk in dataset.archive.stream(file.checksum):
    process_chunk(chunk)

# Get file metadata for checksum
file = dataset.archive.get_file("<checksum>")
print(f"Size: {file.size}, Type: {file.mimetype}")

# Check if a blob exists
if dataset.archive.exists("<checksum>"):
    print("Blob exists")
```

Alternatively, use the shortcut to get the repository directly:

```python
from ftm_lakehouse import lake

archive = lake.get_archive("my_dataset")
file = archive.store("/path/to/document.pdf")
```

## Archiving Blobs

### From Local Path

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

file = dataset.archive.store("/path/to/document.pdf")
print(f"Checksum: {file.checksum}")
print(f"Size: {file.size}")
print(f"MIME type: {file.mimetype}")
```

### From URL

```python
file = dataset.archive.store("https://example.com/report.pdf")
```


## Reading Files

### Open as File-like Handle

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")
with dataset.archive.open("<checksum>") as fh:
    content = fh.read()
```

### Stream Bytes

For large files, streaming is more memory efficient:

```python
for chunk in dataset.archive.stream("<checksum>"):
    process_chunk(chunk)
```

### Get Local Path

For tools that require a local file path, this downloads the blob into a temporary directory which is cleaned up when leaving the context (except if the archive is local, see warning below).

```python
with dataset.archive.local_path("<checksum>") as path:
    # path is a pathlib.Path object
    subprocess.run(["pdftotext", str(path), "output.txt"])
```

!!! warning
    If the archive is local, this returns the actual file path. Do not modify or delete the file at this path.

## File Metadata

### Get File Info

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

file = dataset.archive.get_file(checksum)
if file:
    print(f"Name: {file.name}")
    print(f"Key: {file.key}")
    print(f"Size: {file.size}")
    print(f"MIME type: {file.mimetype}")
    print(f"Checksum: {file.checksum}")
```

### Iterate All Files

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

for file in dataset.archive.iterate_files():
    print(f"{file.key}: {file.checksum}")
```

## File to Entity Conversion

Files can be converted to [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entities:

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Archive a file
file = dataset.archive.store("/path/to/document.pdf")

# Convert to FtM entity
entity = file.to_entity()
print(f"Schema: {entity.schema.name}")  # Document or similar
print(f"Content hash: {entity.first('contentHash')}")

# Add to entity store
dataset.entities.add(entity, origin="archive")
```

## CLI Usage

The CLI provides archive commands under the `archive` subcommand:

```bash
# List all files
ftm-lakehouse -d my_dataset archive ls

# List only checksums
ftm-lakehouse -d my_dataset archive ls --checksums

# List only keys (paths)
ftm-lakehouse -d my_dataset archive ls --keys

# Get file metadata (one json line per File)
ftm-lakehouse -d my_dataset archive head <checksum>

# Retrieve file content
ftm-lakehouse -d my_dataset archive get <checksum> -o output.pdf
```

## Storage Layout

Files are stored in a content-addressable layout:

```
my_dataset/
  archive/
    00/
      de/
        ad/
          00deadbeef123456789012345678901234567890/
            blob                    # file blob (raw bytes)
            {file_id}.json          # metadata (one per source path)
            {origin}.txt            # (optional) extracted text
```

The checksum is split into directory segments for better filesystem performance.

## Archive URL Resolution

Use `Dataset.get_blob_url()` to get a fetchable URL for an archived blob. The URL format is automatically determined by the storage backend:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")
url = dataset.get_blob_url("<checksum>")
```

### Resolution Priority

The first matching strategy wins:

| Priority | Backend | URL Format |
|----------|---------|------------|
| 1 | Public prefix configured | `https://cdn.example.com/archive/ab/cd/ef/<checksum>/blob` |
| 2 | Cloud storage (S3, GCS, Azure) | Presigned URL via fsspec `sign()` |
| 3 | HTTP API mode | API URL with scoped JWT `?token=...` |
| 4 | Local filesystem | `file:///path/to/archive/ab/cd/ef/<checksum>/blob` |

### Public Prefix

If a public URL prefix is configured (e.g. for a CDN or reverse proxy), it is joined with the blob's archive path. This takes priority over all other strategies.

The prefix can be set per-dataset in `config.yml`:

```yaml
name: my_dataset
public_url_prefix: https://cdn.example.com/my_dataset
```

Or globally via environment variable (supports `{{ dataset }}` Jinja-style template):

```bash
export LAKEHOUSE_PUBLIC_URL_PREFIX="https://cdn.example.com/{{ dataset }}"
```

### Cloud Storage (S3, GCS, Azure)

When the lakehouse is backed by a cloud storage provider that supports presigned URLs, `get_blob_url()` generates a time-limited signed URL. This works with any [fsspec](https://filesystem-spec.readthedocs.io/) backend that implements the `sign()` method (S3, GCS, Azure Blob Storage, etc.).

The URL expiration is controlled by `LAKEHOUSE_ARCHIVE_URL_EXPIRE` (in seconds, default: 900 = 15 minutes):

```bash
export LAKEHOUSE_ARCHIVE_URL_EXPIRE=3600  # 1 hour
```

### HTTP API Mode

When running against a remote lakehouse API, `get_blob_url()` returns the API URL with a scoped JWT query parameter. The token is restricted to `GET`/`HEAD` methods on the specific blob path and expires after `LAKEHOUSE_ARCHIVE_URL_EXPIRE` seconds.

### Local Filesystem

For local storage, `get_blob_url()` returns a `file:///` URI pointing directly to the blob on disk.

## Complete Example

```python
from ftm_lakehouse import ensure_dataset


def main():
    dataset = ensure_dataset("documents")

    # Archive some files
    files = []
    for path in ["/path/to/doc1.pdf", "/path/to/doc2.pdf"]:
        file = dataset.archive.put(path)
        files.append(file)
        print(f"Archived: {file.name} ({file.checksum})")

    # Convert to entities
    with dataset.entities.writer(origin="archive") as writer:
        for file in files:
            entity = file.to_entity()
            writer.add_entity(entity)

    dataset.entities.flush()

    # List all archived files
    print("\nAll files:")
    for file in dataset.archive.iterate_files():
        print(f"  - {file.name}: {file.size} bytes")


if __name__ == "__main__":
    main()
```
