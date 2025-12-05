"""Tests for the mapping processor module."""

import pytest

from ftm_lakehouse.conventions.tag import mapping_tag
from ftm_lakehouse.model.mapping import (
    EntityMapping,
    Mapping,
    PropertyMapping,
    mapping_origin,
)


@pytest.fixture
def csv_file(tmp_dataset, fixtures_path):
    """Archive a CSV file and return its metadata."""
    return tmp_dataset.archive.archive_file(fixtures_path / "src" / "companies.csv")


@pytest.fixture
def company_mapping():
    """Create a mapping configuration for the companies CSV."""
    return Mapping(
        entities={
            "company": EntityMapping(
                schema="Company",
                keys=["id"],
                properties={
                    "name": PropertyMapping(column="name"),
                    "jurisdiction": PropertyMapping(column="jurisdiction"),
                    "incorporationDate": PropertyMapping(column="incorporation_date"),
                    "status": PropertyMapping(column="status"),
                },
            )
        }
    )


class TestMappingProcessor:
    """Test the DatasetMappings interface."""

    def test_make_mapping(self, tmp_dataset, csv_file, company_mapping):
        """Test creating a mapping configuration."""
        mappings = tmp_dataset.mappings

        # Create mapping config
        mapping = mappings.make_mapping(
            csv_file.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        assert mapping.content_hash == csv_file.checksum
        assert mapping.dataset == tmp_dataset.name
        assert len(mapping.queries) == 1

        # Verify it's stored
        stored = mappings.get_mapping(csv_file.checksum)
        assert stored is not None
        assert stored.content_hash == csv_file.checksum

    def test_list_mappings(self, tmp_dataset, csv_file, company_mapping):
        """Test listing all mapping configurations."""
        mappings = tmp_dataset.mappings

        # Initially empty
        assert list(mappings.list_mappings()) == []

        # Create mapping
        mappings.make_mapping(
            csv_file.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        # Should now list the mapping
        mapping_list = list(mappings.list_mappings())
        assert len(mapping_list) == 1
        assert csv_file.checksum in mapping_list

    def test_process_mapping(self, tmp_dataset, csv_file, company_mapping):
        """Test processing a mapping configuration."""
        mappings = tmp_dataset.mappings
        entities = tmp_dataset.entities

        # Create mapping config
        mappings.make_mapping(
            csv_file.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        # Process the mapping via high-level interface
        count = mappings.process(csv_file.checksum)

        assert count == 3  # 3 companies in CSV

        # Flush and verify entities
        entities.flush()
        all_entities = list(entities.query())
        assert len(all_entities) == 3

        # Verify entity data
        names = set()
        for entity in all_entities:
            assert entity.schema.name == "Company"
            names.add(entity.first("name"))
            # Should have proof linking to source file
            assert csv_file.checksum in entity.get("proof")

        assert names == {"Acme Corporation", "Global Industries Ltd", "TechStart Inc"}

    def test_process_mapping_origin(self, tmp_dataset, csv_file, company_mapping):
        """Test that processed entities have correct origin."""
        mappings = tmp_dataset.mappings
        entities = tmp_dataset.entities

        mappings.make_mapping(
            csv_file.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        mappings.process(csv_file.checksum)

        entities.flush()
        entities.export_statements()
        entities.export()

        # Check origin in exported entities
        exported = list(entities.iterate())
        assert len(exported) == 3

        expected_origin = mapping_origin(csv_file.checksum)
        for entity in exported:
            origins = entity.context.get("origin", [])
            assert expected_origin in origins

    def test_process_mapping_skip_if_latest(
        self, tmp_dataset, csv_file, company_mapping
    ):
        """Test that processing skips if already up-to-date."""
        mappings = tmp_dataset.mappings

        mappings.make_mapping(
            csv_file.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        # First process
        count1 = mappings.process(csv_file.checksum)
        assert count1 == 3

        # Second process should skip (returns 0)
        count2 = mappings.process(csv_file.checksum)
        assert count2 == 0

    def test_process_mapping_reprocess_after_config_change(self, tmp_dataset, csv_file):
        """Test that processing runs again after mapping config changes."""
        mappings = tmp_dataset.mappings

        # Initial mapping with one set of properties
        initial_mapping = Mapping(
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

        mappings.make_mapping(
            csv_file.checksum,
            queries=[initial_mapping.model_dump(by_alias=True)],
        )

        # First process
        count1 = mappings.process(csv_file.checksum)
        assert count1 == 3

        # Update mapping config with different properties
        updated_mapping = Mapping(
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

        mappings.make_mapping(
            csv_file.checksum,
            queries=[updated_mapping.model_dump(by_alias=True)],
        )

        # Should process again because config changed
        count2 = mappings.process(csv_file.checksum)
        assert count2 == 3  # Processes again

    def test_process_all_mappings(self, tmp_dataset, fixtures_path, company_mapping):
        """Test processing all mappings in a dataset."""
        mappings = tmp_dataset.mappings

        # Archive a file
        file1 = tmp_dataset.archive.archive_file(
            fixtures_path / "src" / "companies.csv"
        )

        # Create mapping for the file
        mappings.make_mapping(
            file1.checksum,
            queries=[company_mapping.model_dump(by_alias=True)],
        )

        # Process all via high-level interface
        results = mappings.process_all()

        assert file1.checksum in results
        assert results[file1.checksum] == 3

    def test_mapping_tag_function(self):
        """Test the mapping_tag helper function."""
        checksum = "abc123def456"
        tag_key = mapping_tag(checksum)
        assert checksum in tag_key
        assert tag_key.startswith("mappings/")
        assert tag_key.endswith("/last_processed")

    def test_mapping_origin_function(self):
        """Test the mapping_origin helper function."""
        checksum = "abc123def456"
        origin = mapping_origin(checksum)
        assert origin == f"mapping:{checksum}"
