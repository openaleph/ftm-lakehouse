# Usage

This guide covers using `ftm-lakehouse` from a tenant (application) perspective.

## Getting Started

```python
from ftm_lakehouse import lake

# Get or create a dataset
dataset = lake.ensure_dataset("my_dataset", title="My Dataset")
```

## Working with Datasets

### Get a Dataset

```python
from ftm_lakehouse import get_dataset

dataset = get_dataset("my_dataset")
```

### Check if Dataset Exists

```python
if dataset.exists():
    print(f"Dataset {dataset.name} exists")
```

### Update Dataset Config

```python
dataset.update_model(
    title="Updated Title",
    summary="A dataset containing leaked documents",
)
```

### Access Dataset Metadata

```python
model = dataset.model
print(model.title)
print(model.name)
```

## Working with Entities

Entities are stored as statements in a journal, then flushed to a parquet store.

### Add a Single Entity

```python
from followthemoney import model

entity = model.make_entity("Person")
entity.make_identifier("john-doe")
entity.set("name", "John Doe")

dataset.entities.add(entity, origin="import")
```

### Bulk Write Entities

```python
with dataset.entities.bulk(origin="import") as writer:
    for entity in source_entities:
        writer.add_entity(entity)
```

### Flush to Parquet Store

```python
count = dataset.entities.flush()
print(f"Flushed {count} statements")
```

### Query Entities

```python
# Query with filters
for entity in dataset.entities.query(origin="import"):
    print(entity.caption)

# Get by ID
entity = dataset.entities.get("entity-id")
```

### Stream from Exported JSON

```python
for entity in dataset.entities.stream():
    process(entity)
```

## Working with Files

The archive stores files with content-addressed storage.

### Archive a File

```python
file = dataset.archive.put("/path/to/document.pdf")
print(f"Archived: {file.checksum}")
```

### Check if File Exists

```python
if dataset.archive.exists(checksum):
    print("File exists")
```

### Get File Metadata

```python
file = dataset.archive.get(checksum)
print(f"Size: {file.size}")
print(f"Mimetype: {file.mimetype}")
```

### Open a File

```python
with dataset.archive.open(file) as fh:
    content = fh.read()
```

### Stream File Content

```python
for chunk in dataset.archive.stream(file):
    process(chunk)
```

### Get Local Path (for external tools)

```python
with dataset.archive.local_path(file) as path:
    subprocess.run(["pdftotext", str(path)])
```

## Custom Dataset Models

Tenants can extend `DatasetModel` with custom fields:

```python
from ftm_lakehouse import lake
from ftm_lakehouse.model import DatasetModel


class MyDatasetModel(DatasetModel):
    project_id: str | None = None
    owner_email: str | None = None
    sensitivity: str = "public"


# Use custom model
dataset = lake.get_dataset("my_data", model_class=MyDatasetModel)

# Access typed fields
model: MyDatasetModel = dataset.model
print(model.project_id)

# Update custom fields
dataset.update_model(
    project_id="proj-123",
    owner_email="alice@example.com",
)
```

## Working with Catalogs

For managing multiple datasets:

```python
from ftm_lakehouse import get_catalog

catalog = get_catalog()

# List all datasets
for dataset in catalog.list_datasets():
    print(dataset.name)

# Create a new dataset
dataset = catalog.create_dataset("new_dataset", title="New Dataset")
```

## Configuration

Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LAKEHOUSE_URI` | `data` | Base storage path |
| `LAKEHOUSE_JOURNAL_URI` | `sqlite:///data/journal.db` | Journal database URI |

Or use S3-compatible storage:

```bash
export LAKEHOUSE_URI=s3://my-bucket/lakehouse
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```
