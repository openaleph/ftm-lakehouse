# Core

Cross-cutting concerns used by all layers.

## Settings

Configuration from environment variables.

```python
from ftm_lakehouse.core.settings import Settings

settings = Settings()
print(settings.uri)          # LAKEHOUSE_URI
print(settings.journal_uri)  # LAKEHOUSE_JOURNAL_URI
```

::: ftm_lakehouse.core.settings.Settings
    options:
        heading_level: 3
        show_root_heading: true

## Path Conventions

Standard paths within the lakehouse.

::: ftm_lakehouse.core.conventions.path
    options:
        heading_level: 3
        members:
            - CONFIG
            - INDEX
            - ARCHIVE
            - STATEMENTS
            - MAPPINGS
            - EXPORTS

## Tag Conventions

Standard tags for freshness tracking.

::: ftm_lakehouse.core.conventions.tag
    options:
        heading_level: 3
        members:
            - JOURNAL_UPDATED
            - STATEMENTS_UPDATED
            - ARCHIVE_UPDATED
            - EXPORTS_STATEMENTS
            - ENTITIES_JSON
            - STATISTICS
