# ftm_lakehouse.lake

Public convenience functions for the lakehouse.

```python
from ftm_lakehouse import lake

dataset = lake.get_dataset("my_data")
dataset = lake.ensure_dataset("my_data", title="My Dataset")
catalog = lake.get_catalog()

# Repository shortcuts
entities = lake.get_entities("my_data")
archive = lake.get_archive("my_data")
mappings = lake.get_mappings("my_data")
```

::: ftm_lakehouse.lake.get_catalog
    options:
        heading_level: 2

::: ftm_lakehouse.lake.get_dataset
    options:
        heading_level: 2

::: ftm_lakehouse.lake.ensure_dataset
    options:
        heading_level: 2

## Repository Shortcuts

::: ftm_lakehouse.lake.get_entities
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_archive
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_mappings
    options:
        heading_level: 3

## Classes

::: ftm_lakehouse.catalog.Catalog
    options:
        heading_level: 3
        members:
            - get_dataset
            - list_datasets
            - create_dataset
            - model
            - update_model

::: ftm_lakehouse.dataset.Dataset
    options:
        heading_level: 3
        members:
            - archive
            - entities
            - mappings
            - jobs
            - model
            - update_model
            - exists
            - ensure
