# logic

The logic module contains pure stateless transformation functions with no infrastructure dependencies. Functions here take inputs and produce outputs without side effects.

## Entity Aggregation

Aggregate statement streams into FollowTheMoney entities:

```python
from ftm_lakehouse.logic import aggregate_statements
from followthemoney.statement.serialize import read_csv_statements

with open("statements.csv") as f:
    statements = read_csv_statements(f)
    for entity in aggregate_statements(statements, "my_dataset"):
        print(f"{entity.id}: {entity.caption}")
```

::: ftm_lakehouse.logic.aggregate_statements
    options:
        heading_level: 3
        show_root_heading: true

## Mapping Processing

Generate entities from FollowTheMoney mapping configurations:

```python
from ftm_lakehouse.logic import map_entities
from ftm_lakehouse.model.mapping import DatasetMapping

mapping = DatasetMapping(
    dataset="my_dataset",
    content_hash="abc123...",
    queries=[...]
)

for entity in map_entities(mapping, csv_path):
    print(f"{entity.schema.name}: {entity.caption}")
```

::: ftm_lakehouse.logic.map_entities
    options:
        heading_level: 3
        show_root_heading: true

## Statement Serialization

Pack and unpack statements for efficient storage:

```python
from ftm_lakehouse.logic import pack_statement, unpack_statement
from followthemoney import Statement

# Pack a statement to string
packed = pack_statement(stmt)

# Unpack back to Statement
stmt = unpack_statement(packed)
```

### pack_statement

```python
def pack_statement(stmt: Statement) -> str
```

Pack a Statement into a null-byte joined string for compact storage.

**Args:**

- `stmt`: A FollowTheMoney Statement object

**Returns:** Serialized string representation

### unpack_statement

```python
def unpack_statement(data: str) -> Statement
```

Unpack a null-byte joined string back into a Statement.

**Args:**

- `data`: Serialized statement string from `pack_statement`

**Returns:** Reconstructed Statement object
