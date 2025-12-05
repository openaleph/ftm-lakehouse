"""Dataset mappings interface for CSV/tabular data transformations."""

from typing import TYPE_CHECKING, Generator

from anystore.types import SDict
from ftmq.types import Entities

from ftm_lakehouse.conventions import path, tag
from ftm_lakehouse.core.decorators import versioned
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.logic.mappings import map_entities as _map_entities
from ftm_lakehouse.model.mapping import DatasetMapping, mapping_origin
from ftm_lakehouse.util import check_dataset, load_model

if TYPE_CHECKING:
    from ftm_lakehouse.lake.entities import DatasetEntities
    from ftm_lakehouse.service import DatasetArchive


class DatasetMappings(LakeMixin):
    """Interface for managing and processing mapping configurations."""

    def __init__(
        self,
        archive: "DatasetArchive",
        entities: "DatasetEntities",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.archive = archive
        self.entities = entities

    def make_mapping(self, content_hash: str, **data: SDict) -> DatasetMapping:
        """Get a mapping configuration for the given csv/xls/tabular file by its
        SHA1, optionally with patched data"""

        @versioned(path.mapping_yml(content_hash))
        def _make_mapping(self, content_hash) -> DatasetMapping:
            if not self.archive.exists(content_hash):
                raise FileNotFoundError(
                    f"File with SHA1 `{content_hash}` does not exist in archive."
                )
            mapping_path = path.mapping_yml(content_hash)
            if self.storage.exists(mapping_path):
                raw = self.storage.get(mapping_path)
                mapping = load_model(mapping_path, raw, DatasetMapping)
            else:
                mapping = DatasetMapping(
                    dataset=self.name, content_hash=content_hash, queries=[]
                )
            patched = {**mapping.model_dump(by_alias=True), **data}
            patched["dataset"] = check_dataset(self.name, patched)
            patched["content_hash"] = content_hash
            return DatasetMapping(**patched)

        with self.tags.touch(tag.mapping_config_tag(content_hash)):
            return _make_mapping(self, content_hash)

    def map_entities(self, content_hash: str) -> Entities:
        """Generate entities from a mapping configuration."""
        mapping = self.get_mapping(content_hash)
        if mapping is None:
            raise FileNotFoundError(f"No mapping configuration for `{content_hash}`")
        file = self.archive.lookup_file(content_hash)
        with self.archive.local_path(file) as csv_path:
            yield from _map_entities(mapping, csv_path)

    def list_mappings(self) -> Generator[str, None, None]:
        """
        List all content hashes that have mapping configurations.

        Yields:
            Content hash strings for files with mapping.yml configs
        """
        prefix = f"{path.MAPPINGS}/"
        for key in self.storage.iterate_keys(prefix=prefix):
            # Keys look like: mappings/<content_hash>/mapping.yml
            if key.endswith(f"/{path.MAPPING}"):
                parts = key.split("/")
                if len(parts) >= 3:
                    yield parts[1]  # content_hash

    def get_mapping(self, content_hash: str) -> DatasetMapping | None:
        """
        Get an existing mapping configuration without creating one.

        Args:
            content_hash: SHA1 checksum of the source file

        Returns:
            DatasetMapping if exists, None otherwise
        """
        mapping_path = path.mapping_yml(content_hash)
        if not self.storage.exists(mapping_path):
            return None
        raw = self.storage.get(mapping_path)
        return load_model(mapping_path, raw, DatasetMapping)

    def process(self, content_hash: str) -> int:
        """
        Process a mapping configuration and store generated entities.

        Skips processing if the mapping output is already up-to-date relative
        to the mapping config.

        Args:
            content_hash: SHA1 checksum of the source file

        Returns:
            Number of entities generated (0 if skipped)
        """
        tag_key = tag.mapping_tag(content_hash)
        config_tag = tag.mapping_config_tag(content_hash)

        if self.tags.is_latest(tag_key, [config_tag]):
            self.log.info(
                "Mapping already up-to-date, skipping ...",
                content_hash=content_hash,
            )
            return 0

        origin = mapping_origin(content_hash)
        count = 0

        with self.tags.touch(tag_key):
            self.log.info("Processing mapping ...", content_hash=content_hash)
            with self.entities.bulk(origin=origin) as bulk:
                for entity in self.map_entities(content_hash):
                    bulk.add_entity(entity)
                    count += 1
            self.log.info(
                "Mapping complete.", content_hash=content_hash, entities=count
            )

        return count

    def process_all(self) -> dict[str, int]:
        """
        Process all mapping configurations in the dataset.

        Returns:
            Dict mapping content_hash to number of entities generated
        """
        results = {}
        for content_hash in self.list_mappings():
            results[content_hash] = self.process(content_hash)
        return results
