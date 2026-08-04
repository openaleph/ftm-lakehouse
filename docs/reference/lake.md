# ftm_lakehouse.lake

Public convenience functions for the lakehouse – repositories are the dataset handle.

```python
from ftm_lakehouse import ensure_dataset, get_entities, get_archive, get_lakehouse

ensure_dataset("my_data", title="My Dataset", shards=8)

entities = get_entities("my_data")
archive = get_archive("my_data")

for name in get_lakehouse().list_datasets():
    ...
```

::: ftm_lakehouse.lake.get_lakehouse
    options:
        heading_level: 2

## Dataset config lifecycle

::: ftm_lakehouse.catalog.ensure_dataset
    options:
        heading_level: 3

::: ftm_lakehouse.catalog.update_dataset
    options:
        heading_level: 3

::: ftm_lakehouse.catalog.get_dataset_model
    options:
        heading_level: 3

::: ftm_lakehouse.catalog.get_dataset_index
    options:
        heading_level: 3

::: ftm_lakehouse.catalog.dataset_exists
    options:
        heading_level: 3

## Repository Shortcuts

::: ftm_lakehouse.repository.factories.get_entities
    options:
        heading_level: 3

::: ftm_lakehouse.repository.factories.get_archive
    options:
        heading_level: 3

::: ftm_lakehouse.repository.factories.get_documents
    options:
        heading_level: 3

## Custom dataset models

::: ftm_lakehouse.model.dataset.set_model_class
    options:
        heading_level: 3

## Classes

::: ftm_lakehouse.catalog.Catalog
    options:
        heading_level: 3
        members:
            - dataset_uri
            - list_datasets
            - ensure_dataset
