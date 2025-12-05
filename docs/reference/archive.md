# ftm_lakehouse.service.archive

The archive module provides content-addressable file storage.

## DatasetArchive

The main interface for file operations on a dataset:

```python
from ftm_lakehouse import get_archive

archive = get_archive("my_dataset")

# Archive a file
file = archive.archive_file("/path/to/document.pdf")

# Check if file exists
exists = archive.exists(checksum)

# Get file metadata
file = archive.lookup_file(checksum)

# Open file
with archive.open_file(file) as fh:
    content = fh.read()

# Stream file
for chunk in archive.stream_file(file):
    process(chunk)

# Get local path
with archive.local_path(file) as path:
    subprocess.run(["process", str(path)])

# Iterate all files
for file in archive.iter_files():
    print(f"{file.key}: {file.checksum}")
```

::: ftm_lakehouse.service.archive.DatasetArchive
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - exists
            - lookup_file
            - stream_file
            - open_file
            - local_path
            - iter_files
            - archive_file
            - delete_file
