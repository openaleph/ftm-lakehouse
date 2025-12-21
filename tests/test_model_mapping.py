from followthemoney.mapping.query import QueryMapping

from ftm_lakehouse.model.mapping import (
    DatasetMapping,
    EntityMapping,
    Mapping,
    PropertyMapping,
    mapping_origin,
)


def test_model_mapping():
    """Test Mapping model."""
    mapping = Mapping(
        entities={
            "company": EntityMapping(
                schema="Company",
                keys=["id"],
                properties={
                    "name": PropertyMapping(column="name"),
                },
            )
        }
    )
    assert "company" in mapping.entities
    assert mapping.entities["company"].schema_ == "Company"
    assert mapping.filters == {}
    assert mapping.filters_not == {}

    dataset_mapping = DatasetMapping(
        dataset="test_dataset",
        content_hash="abc123def456",
        queries=[mapping],
    )
    assert dataset_mapping.dataset == "test_dataset"
    assert dataset_mapping.content_hash == "abc123def456"
    assert len(dataset_mapping.queries) == 1
    assert dataset_mapping.queries[0].entities["company"].schema_ == "Company"


def test_model_mapping_yml(fixtures_path):
    """Test loading original ftm mapping yml to pydantic model"""
    mapping = DatasetMapping.from_yaml_uri(fixtures_path / "mapping.yml")
    assert mapping.dataset == "test_dataset"
    assert mapping.content_hash == "abc123def456"
    assert len(mapping.queries) == 1
    assert mapping.queries[0].entities["company"].schema_ == "Company"
    assert mapping.queries[0].entities["company"].properties["name"].column == "name"
    assert mapping.queries[0].entities["company"].properties["name"].literal is None


def test_model_mapping_origin():
    """Test the mapping_origin helper function."""
    checksum = "abc123def456"
    origin = mapping_origin(checksum)
    assert origin == "mapping:abc123def456"


def test_model_mapping_to_ftm_query_mapping():
    """Test conversion to followthemoney QueryMapping."""
    mapping = Mapping(
        entities={
            "company": EntityMapping(
                schema="Company",
                keys=["id"],
                properties={
                    "name": PropertyMapping(column="name"),
                    "jurisdiction": PropertyMapping(column="jurisdiction"),
                    "incorporationDate": PropertyMapping(column="inc_date"),
                },
            ),
            "owner": EntityMapping(
                schema="Person",
                keys=["owner_id"],
                properties={
                    "name": PropertyMapping(column="owner_name"),
                },
            ),
        }
    )

    query_mapping = mapping.make_mapping("abc123", dataset="test")

    assert isinstance(query_mapping, QueryMapping)
    assert "abc123" in query_mapping.source.urls

    # Check entities are properly configured
    entities = list(query_mapping.entities)
    assert len(entities) == 2

    schema_names = {e.schema.name for e in entities}
    assert schema_names == {"Company", "Person"}

    # Check entity keys are set
    entity_by_name = {e.name: e for e in entities}
    assert entity_by_name["company"].keys == ["id"]
    assert entity_by_name["owner"].keys == ["owner_id"]

    # Test conversion with filters.
    mapping = Mapping(
        entities={
            "company": EntityMapping(
                schema="Company",
                keys=["id"],
                properties={"name": PropertyMapping(column="name")},
            ),
        },
        filters={"status": "active"},
        filters_not={"dissolved": "true"},
    )

    query_mapping = mapping.make_mapping("abc123", dataset="test")

    assert dict(query_mapping.source.filters) == {"status": "active"}
    assert dict(query_mapping.source.filters_not) == {"dissolved": "true"}
