# ftm_lakehouse.lake

The lake module provides the core dataset and catalog management.

## Lakehouse

A lakehouse holds one or more datasets:

```python
from ftm_lakehouse import get_lakehouse

lake = get_lakehouse()

# List datasets
for dataset in lake.get_datasets():
    print(dataset.name)

# Get a specific dataset
dataset = lake.get_dataset("my_dataset")
```

::: ftm_lakehouse.lake.Lakehouse
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - get_dataset
            - get_datasets
            - make_config
            - make_index
            - model

## DatasetLakehouse

A single dataset within the lakehouse:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Ensure dataset exists
dataset.ensure()

# Access components
dataset.entities  # Entity operations
dataset.archive   # File archive
dataset.jobs      # Job tracking

# Update metadata
dataset.make_config(title="New Title")

# Run full update
dataset.make()
```

::: ftm_lakehouse.lake.DatasetLakehouse
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - exists
            - ensure
            - model
            - load_model
            - make_config
            - make_index
            - get_statistics
            - make
            - archive
            - entities
            - jobs

## Factory Functions

```python
from ftm_lakehouse import get_lakehouse, get_dataset, get_archive, get_entities, get_mappings

# Get the lakehouse
lake = get_lakehouse(uri="s3://bucket/lake")

# Get a dataset directly
dataset = get_dataset("my_dataset")

# Get components directly
archive = get_archive("my_dataset")
entities = get_entities("my_dataset")
mappings = get_mappings("my_dataset")
```

::: ftm_lakehouse.lake.get_lakehouse
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_dataset
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_archive
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_entities
    options:
        heading_level: 3

::: ftm_lakehouse.lake.get_mappings
    options:
        heading_level: 3
