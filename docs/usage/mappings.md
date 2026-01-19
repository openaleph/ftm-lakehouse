# Working with Mappings

The mappings repository transforms tabular data (CSV, Excel) into [FollowTheMoney](https://followthemoney.tech) entities using declarative YAML mapping configurations.

## Overview

Mappings allow you to:

- **Define schemas**: Specify which FTM entity types to create from rows
- **Map columns**: Connect CSV columns to entity properties
- **Generate keys**: Create stable entity IDs from row data
- **Track provenance**: Link generated entities back to source files

For complete mapping syntax documentation, see the [FollowTheMoney mappings guide](https://followthemoney.tech/docs/mappings/).

## Quick Start

```python
from ftm_lakehouse import ensure_dataset

dataset = ensure_dataset("my_dataset")

# Archive a CSV file
file = dataset.archive.put("companies.csv")

# Create the mapping configuration
dataset.mappings.make(
    file.checksum,
    queries=[{
        "entities": {
            "company": {
                "schema": "Company",
                "keys": ["id"],
                "properties": {
                    "name": {"column": "name"},
                    "jurisdiction": {"column": "country"},
                }
            }
        }
    }],
)

# Process the mapping
count = dataset.mappings.process(file.checksum)
print(f"Generated {count} entities")

# Flush to storage
dataset.entities.flush()
```

Alternatively, use the shortcut to get the repository directly:

```python
from ftm_lakehouse import lake

mappings = lake.get_mappings("my_dataset")
mapping = mappings.get(content_hash)
```

## Mapping Configuration

Mapping configurations follow the [FollowTheMoney YAML mapping format](https://followthemoney.tech/docs/mappings/). Each mapping contains one or more queries that define how to transform rows into entities.

### Basic Structure

```yaml
entities:
  company:
    schema: Company
    keys:
      - registration_number
    properties:
      name:
        column: company_name
      jurisdiction:
        column: country
      registrationNumber:
        column: registration_number
      incorporationDate:
        column: incorporation_date
```

## Processing Mappings

### Process a Single Mapping

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Process by content hash
count = dataset.mappings.process(file.checksum)
print(f"Generated {count} entities")
```

### Process All Mappings

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# Process all mapping configurations in the dataset
results = dataset.mappings.process_all()

for content_hash, count in results.items():
    print(f"{content_hash}: {count} entities")
```

### Skip Logic

Processing automatically skips if already up-to-date:

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

# First run: processes and generates entities
count1 = dataset.mappings.process(file.checksum)  # Returns 100

# Second run: skips (returns 0)
count2 = dataset.mappings.process(file.checksum)  # Returns 0

# After updating the mapping config, processing runs again
dataset.mappings.make(file.checksum, queries=[updated_config])
count3 = dataset.mappings.process(file.checksum)  # Processes again
```

## Managing Mapping Configs

### List Mappings

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

for content_hash in dataset.mappings.list():
    print(content_hash)
```

### Get Mapping Config

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

mapping = dataset.mappings.get(file.checksum)
if mapping:
    print(f"Dataset: {mapping.dataset}")
    print(f"Queries: {len(mapping.queries)}")
```

## Origin and Provenance

Entities generated from mappings have automatic provenance tracking. The origin is set to `mapping:{content_hash}`, and entities include a `proof` property linking to the source file.

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")

for entity in dataset.entities.query():
    # Origin tracks where data came from
    origins = entity.context.get("origin", [])

    # Proof links to source file checksum
    proof = entity.get("proof")
```

## Storage Layout

Mapping configurations are stored alongside archive files:

```
dataset/
  mappings/
    {content_hash}/
      mapping.yml       # Mapping configuration
```

Tags track processing state:

```
dataset/
  tags/
    mappings/{content_hash}/last_processed    # When mapping was last run
    mappings/{content_hash}/config_updated    # When config was last changed
```

## Complete Example

```python
from ftm_lakehouse import ensure_dataset


def main():
    dataset = ensure_dataset("company_registry")

    # Archive the source CSV
    file = dataset.archive.put("companies.csv")
    print(f"Archived: {file.checksum}")

    # Create mapping configuration
    dataset.mappings.make(
        file.checksum,
        queries=[{
            "entities": {
                "company": {
                    "schema": "Company",
                    "keys": ["registration_number"],
                    "properties": {
                        "name": {"column": "company_name"},
                        "jurisdiction": {"column": "country"},
                        "registrationNumber": {"column": "registration_number"},
                        "incorporationDate": {"column": "incorporation_date"},
                    }
                }
            }
        }],
    )

    # Process the mapping
    count = dataset.mappings.process(file.checksum)
    print(f"Generated {count} entities")

    # Flush to storage
    dataset.entities.flush()

    # Query the generated entities
    print("\nCompanies:")
    for entity in dataset.entities.query(origin=f"mapping:{file.checksum}"):
        print(f"  - {entity.caption}")


if __name__ == "__main__":
    main()
```
