"""Mapping configuration models."""

from anystore.model import BaseModel
from followthemoney import model
from followthemoney.mapping.query import QueryMapping
from pydantic import ConfigDict, Field


class PropertyMapping(BaseModel):
    """Mapping configuration for a single property."""

    column: str | None = None
    columns: list[str] | None = None
    join: str | None = None
    split: str | None = None
    entity: str | None = None
    format: str | None = None
    fuzzy: str | None = None
    required: bool | None = False
    literal: str | None = None
    literals: list[str] | None = None
    template: str | None = None


class EntityMapping(BaseModel):
    """Mapping configuration for a single entity type."""

    key: str | None = None
    keys: list[str] | None = []
    key_literal: str | None = None
    id_column: str | None = None
    schema_: str = Field(..., alias="schema")
    properties: dict[str, PropertyMapping] = {}


class Mapping(BaseModel):
    """A single mapping query configuration."""

    entities: dict[str, EntityMapping] = {}
    filters: dict[str, str] = {}
    filters_not: dict[str, str] = {}

    def make_mapping(self, content_hash: str, dataset: str) -> QueryMapping:
        """Create a FtM QueryMapping from this configuration."""
        mapping_data = self.model_dump(by_alias=True)
        mapping_data.pop("database", None)
        mapping_data["csv_url"] = content_hash
        return model.make_mapping(mapping_data, key_prefix=dataset)


class DatasetMapping(BaseModel):
    """A complete mapping configuration for a dataset file."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset: str
    content_hash: str
    queries: list[Mapping]


def mapping_origin(content_hash: str) -> str:
    """Generate origin identifier for a mapping."""
    return f"mapping:{content_hash}"
