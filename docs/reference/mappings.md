# mappings

## DatasetMappings

The mapping interface for CSV/tabular data transformations:

```python
from ftm_lakehouse import get_mappings

mappings = get_mappings("my_dataset")

# Create or update a mapping configuration
mapping = mappings.make_mapping(content_hash, queries=[...])

# Process a single mapping
count = mappings.process(content_hash)

# Process all mappings
results = mappings.process_all()

# List existing mappings
for content_hash in mappings.list_mappings():
    print(content_hash)
```

::: ftm_lakehouse.lake.mappings.DatasetMappings
    options:
        heading_level: 2
        show_root_heading: false
        members:
            - make_mapping
            - get_mapping
            - list_mappings
            - map_entities
            - process
            - process_all

## Mapping Models

::: ftm_lakehouse.model.mapping.DatasetMapping
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.mapping.Mapping
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.mapping.EntityMapping
    options:
        heading_level: 3
        show_root_heading: true

::: ftm_lakehouse.model.mapping.PropertyMapping
    options:
        heading_level: 3
        show_root_heading: true

## Helper Functions

::: ftm_lakehouse.model.mapping.mapping_origin
    options:
        heading_level: 3

::: ftm_lakehouse.conventions.tag.mapping_tag
    options:
        heading_level: 3

::: ftm_lakehouse.conventions.tag.mapping_config_tag
    options:
        heading_level: 3
