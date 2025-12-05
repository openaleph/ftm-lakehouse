# Working with Files

The archive interface manages source documents and files in `ftm-lakehouse`. It provides content-addressable storage with automatic deduplication.

## Overview

The archive stores files using their SHA1 checksum as the key. This design enables:

- **Deduplication**: Identical files are stored only once (per dataset)
- **Integrity**: Verify file contents via checksum
- **Metadata**: Track file properties (name, size, MIME type, etc.)
- **Provenance**: Link files to [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entities

## Quick Start

```python
from ftm_lakehouse import get_archive

archive = get_archive("my_dataset")

# Archive a file
file = archive.archive_file("/path/to/document.pdf")
print(f"Archived: {file.name} ({file.checksum})")

# Add file to archive from HTTP URL
file = archive.archive_file("https://example.com/report.pdf")
print(f"Archived: {file.name} ({file.checksum})")

# Retrieve file content
with archive.open_file(file) as fh:
    content = fh.read()

# Stream bytes (memory efficient for large files)
for chunk in archive.stream_file(file):
    process_chunk(chunk)

# Get file metadata
file = archive.lookup_file("da39a3ee5e6b4b0d3255bfef95601890afd80709")
print(f"Size: {file.size}, Type: {file.mimetype}")

# Check if file with sha1 checksum exists
assert archive.exists("da39a3ee5e6b4b0d3255bfef95601890afd80709")
```

## Using higher level io helper

The `ftm_lakehouse.io` module provides high level interface to the archive. Instead of using a `File` object to pass to the archive lookup methods (as seen above), simply pass the checksum as string. This shorthand interface is intended to use in client applications.

```python
from ftm_lakehouse import io

checksum = "abc123..."

# Open as file handle
with io.open_file("my_dataset", checksum) as fh:
    content = fh.read()
    # Process content...

# Stream bytes (memory efficient for large files)
for chunk in io.stream_file("my_dataset", checksum):
    process_chunk(chunk)
```

### Get Local Path

For tools that require a local file path:

```python
from ftm_lakehouse import io

# Get a (temporary) local path
with io.get_local_path("my_dataset", checksum) as path:
    # path is a pathlib.Path object
    subprocess.run(["pdftotext", str(path), "output.txt"])
```

!!! warning
    If the archive is local, this returns the actual file path.
    Do not modify or delete the file at this path.

## File Metadata

### Lookup File Info

```python
from ftm_lakehouse import io

file = io.lookup_file("my_dataset", checksum)
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

for file in dataset.archive.iter_files():
    print(f"{file.key}: {file.checksum}")
```

## File to Entity Conversion

Files can be converted to [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/) entities. In the following example, the _dataset_ interface is used to access both the _archive_ service and the _entities_ service.

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Archive a file
file = dataset.archive.archive_file("/path/to/document.pdf")

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

# Get file metadata
ftm-lakehouse -d my_dataset archive head <checksum>

# Retrieve file content
ftm-lakehouse -d my_dataset archive get <checksum> -o output.pdf
```

## Storage Layout

Files are stored in a content-addressable layout:

```bash
my_dataset/
  archive/
    00/
      de/
        ad/
          00deadbeef123456789012345678901234567890               # file blob
          00deadbeef123456789012345678901234567890.json          # metadata
          00deadbeef123456789012345678901234567890.<origin>.txt  # (optional) extracted text
```

The checksum is split into directory segments for better filesystem performance.
