# Working with Files

The archive repository manages source documents and files metadata in `ftm-lakehouse`. It provides content-addressable storage with automatic deduplication.

## Overview

The archive stores files using their SHA1 checksum as the key. This design enables:

- **Deduplication**: Identical files are stored only once (per dataset)
- **Integrity**: Verify file contents via checksum
- **Metadata**: Track file properties (name, size, MIME type, etc.)
- **Provenance**: Link files to [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entities


!!! info "Blob vs. File object"
    When referring to a _Blob_, this is the actual bytes content of a given source file, identified by it's SHA1 checksum.

    When referring to a _File_, this is the metadata [File][ftm_lakehouse.model.File] model. Multiple metadata files can exist for a single bytes blob.


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
    If the archive is local, this returns the actual file path.
    Do not modify or delete the file at this path.

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
    with dataset.entities.bulk(origin="archive") as writer:
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
