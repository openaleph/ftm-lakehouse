"""Tests for MappingOperation - CSV to entities workflow."""

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.mapping import (
    DatasetMapping,
    EntityMapping,
    Mapping,
    PropertyMapping,
    mapping_origin,
)
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.repository import ArchiveRepository, MappingRepository

DATASET = "mapping_test"


def test_operation_mapping(fixtures_path, tmp_path):
    """Test MappingOperation: CSV to entities workflow with tags and origin."""
    # Archive the CSV file first
    archive = ArchiveRepository(dataset=DATASET, uri=tmp_path)
    csv_file = archive.store(fixtures_path / "src/companies.csv", origin="test")
    content_hash = csv_file.checksum

    # Create mapping configuration
    mappings = MappingRepository(dataset=DATASET, uri=tmp_path)
    mapping_config = DatasetMapping(
        dataset=DATASET,
        content_hash=content_hash,
        queries=[
            Mapping(
                entities={
                    "company": EntityMapping(
                        schema="Company",
                        keys=["id"],
                        properties={
                            "name": PropertyMapping(column="name"),
                            "jurisdiction": PropertyMapping(column="jurisdiction"),
                        },
                    )
                }
            )
        ],
    )
    mappings.put(mapping_config)

    # No target tag before run
    target_path = f"tags/lakehouse/mappings/{content_hash}/last_processed"
    assert not (tmp_path / target_path).exists()

    # Create operation and verify target/dependencies
    job = MappingJob.make(dataset=DATASET, content_hash=content_hash)
    op = MappingOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == tag.mapping_tag(content_hash)
    assert op.get_target() == f"mappings/{content_hash}/last_processed"
    assert op.get_dependencies() == [path.mapping(content_hash)]
    assert op.get_dependencies() == [f"mappings/{content_hash}/mapping.yml"]

    # Run the mapping operation
    result = op.run()

    assert result.done == 3  # 3 companies in CSV
    assert result.running is False
    assert result.stopped is not None

    # Verify tag exists at hardcoded path after run
    assert (tmp_path / target_path).exists()
    assert (tmp_path / f"mappings/{content_hash}/mapping.yml").exists()

    # Verify entities were created with correct origin
    origin = mapping_origin(content_hash)
    entities = list(op.entities.query(origin=origin))
    assert len(entities) == 3

    names = {e.first("name") for e in entities}
    assert names == {"Acme Corporation", "Global Industries Ltd", "TechStart Inc"}

    for entity in entities:
        assert entity.schema.name == "Company"
        assert content_hash in entity.get("proof")

    # Query all entities (flush_first to ensure we get everything)
    all_entities = list(op.entities.query(flush_first=True))
    assert len(all_entities) == 3

    # Verify origin is tracked on entities
    for entity in all_entities:
        # StatementEntity tracks origins via to_context_dict()
        ctx = entity.to_context_dict()
        origins = ctx.get("origin", [])
        assert origin in origins
        assert f"mapping:{content_hash}" in origins
