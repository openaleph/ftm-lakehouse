# ftm_lakehouse.io

The `io` module provides high-level functions for working with entities and files.

This is the recommended API for client applications:

```python
from ftm_lakehouse import io

# Work with entities
entity = io.get_entity("dataset", "entity-id")
io.write_entities("dataset", entities, origin="import")

# Work with files
file = io.archive_file("dataset", "/path/to/file.pdf")
with io.open_file("dataset", checksum) as fh:
    content = fh.read()
```

::: ftm_lakehouse.io
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - ensure_dataset
            - get_entity
            - entity_writer
            - write_entities
            - write_entity
            - flush
            - stream_entities
            - iterate_entities
            - lookup_file
            - stream_file
            - open_file
            - archive_file
            - get_local_path
            - get_dataset_metadata
            - update_dataset_metadata
            - exists
