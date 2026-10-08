# Working with Files

The archive repository stores source files and their metadata, content-addressed by SHA256 checksum: identical files are stored once per dataset, and every file can be verified against its checksum and turned into a [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entity.

!!! info "Blob vs. File object"
    A _Blob_ is the bytes of a source file, identified by its SHA256 checksum.

    A _File_ is the metadata [File](../reference/model.md#ftm_lakehouse.model.file.File) model. One blob can have several File objects – one per source path it was archived from.

## Archiving Files

`store` takes a local path or a URL and returns the `File` metadata:

```python
from ftm_lakehouse import ensure_dataset, get_archive

ensure_dataset("my_dataset")
archive = get_archive("my_dataset")

file = archive.store("/path/to/document.pdf")
file = archive.store("https://example.com/report.pdf")
print(file.name, file.checksum, file.size, file.mimetype)
```

## Reading Files

```python
# File-like handle
with archive.open(file.checksum) as fh:
    content = fh.read()

# Chunks, for large files
for chunk in archive.stream(file.checksum):
    process_chunk(chunk)

# A local path, for tools that need one
with archive.local_path(file.checksum) as path:
    subprocess.run(["pdftotext", str(path), "output.txt"])
```

`local_path` copies a remote blob to a temporary file, removed when the context exits.

!!! warning
    On a local archive `local_path` returns the blob's actual path. Do not modify or delete it.

## File Metadata

```python
archive.exists(checksum)                    # is the blob there?
file = archive.get_file(checksum)           # first File, FileNotFoundError if none
files = list(archive.get_all_files(checksum))  # every File of the blob

for file in archive.iterate_files():        # every File in the archive
    print(f"{file.key}: {file.checksum}")
```

## File to Entity

```python
from ftm_lakehouse import get_entities

entity = file.to_entity()
print(entity.schema.name, entity.first("contentHash"))

entities = get_entities("my_dataset")
entities.add(entity, origin="archive")
entities.flush()
```

## CLI Usage

```bash
# List all files (one json line per File), or only checksums / keys
ftm-lakehouse -d my_dataset archive ls
ftm-lakehouse -d my_dataset archive ls --checksums
ftm-lakehouse -d my_dataset archive ls --keys

# Every File of a checksum, as json lines
ftm-lakehouse -d my_dataset archive head <checksum>

# Retrieve file content
ftm-lakehouse -d my_dataset archive get <checksum> -o output.pdf

# Download every file to a directory, under the paths from exports/documents.csv
ftm-lakehouse -d my_dataset archive download -o ./files
```

## Storage Layout

The checksum is split into directory segments:

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

The full dataset layout is in [Conventions](../conventions.md).

## Public Blob URLs

A public URL prefix (e.g. a CDN, or the nginx in front of the lakehouse API) is joined with the blob's archive path – `https://cdn.example.com/my_dataset/archive/ab/cd/ef/<checksum>/blob` – and written as `public_url` into the documents export (`documents.csv`) and the `index.json` resource links.

Per dataset in `config.yml`:

```yaml
name: my_dataset
public_url_prefix: https://cdn.example.com/my_dataset
```

Or globally via environment variable (supports a `${dataset}` placeholder):

```bash
export LAKEHOUSE_PUBLIC_URL_PREFIX="https://cdn.example.com/\${dataset}"
```

Without a prefix, the lakehouse API serves blobs under `/{dataset}/archive/...`; access control is up to the reverse proxy (see [API deployment](../deployment/api.md)).
