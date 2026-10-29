# Working with Files

The archive repository stores source files as blobs, content-addressed by SHA256 checksum: identical files are stored once per dataset, and every file can be verified against its checksum. A file's name, path and type live in its [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entity; anything else a caller wants to keep goes next to the blob as [raw data](#extra-metadata).

## Archiving Files

`store` takes a local path or a URL and returns the checksum:

```python
from ftm_lakehouse import ensure_dataset, get_archive

ensure_dataset("my_dataset")
archive = get_archive("my_dataset")

checksum = archive.store("/path/to/document.pdf")
checksum = archive.store("https://example.com/report.pdf")
```

## Reading Files

```python
# File-like handle
with archive.open(checksum) as fh:
    content = fh.read()

# Chunks, for large files
for chunk in archive.stream(checksum):
    process_chunk(chunk)

# A local path, for tools that need one
with archive.local_path(checksum) as path:
    subprocess.run(["pdftotext", str(path), "output.txt"])
```

`local_path` copies a remote blob to a temporary file, removed when the context exits.

!!! warning
    On a local archive `local_path` returns the blob's actual path. Do not modify or delete it.

## Listing Files

```python
archive.exists(checksum)                    # is the blob there?

for checksum in archive.iterate_checksums():  # every blob in the archive
    print(checksum)
```

## Extra Metadata

Callers keep their own metadata about a file – where it came from, who crawled it – next to its blob: `put_data` writes raw bytes at a relative path in the blob's directory, `get_data` reads them back:

```python
import json

archive.put_data(checksum, "my_crawler.json", json.dumps({"url": url}).encode())
meta = json.loads(archive.get_data(checksum, "my_crawler.json"))
```

`blob` and top-level `*.txt` paths are reserved – the content and the extracted text (`put_txt` / `get_txt`).

## File to Entity

`make_file_entities` builds the `Document` entity of an archived file – its schema picked by mime type – after the `Folder` entities of its path. The `info` is the file's anystore `Stats`: `store.info(key)` for a file in a crawled store (the key is its path), `UriResource(uri).info()` for a single uri (the key is just its file name).

```python
from anystore.store.resource import UriResource

from ftm_lakehouse import get_entities
from ftm_lakehouse.helpers.file import make_file_entities

uri = "/path/to/document.pdf"
checksum = archive.store(uri)

entities = get_entities("my_dataset")
info = UriResource(uri).info()
entities.add_many(make_file_entities(info, checksum, "my_dataset"), origin="archive")
entities.flush()
```

## CLI Usage

```bash
# List the checksums of all files
ftm-lakehouse -d my_dataset archive ls

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
