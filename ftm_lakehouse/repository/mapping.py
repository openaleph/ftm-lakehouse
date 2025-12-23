"""MappingRepository - mapping configuration storage using VersionStore."""

from typing import Generator

from anystore.logging import get_logger
from anystore.types import Uri

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.mapping import DatasetMapping
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage.versions import VersionedModelStore

log = get_logger(__name__)


class MappingRepository(BaseRepository):
    """
    Repository for mapping configuration storage.

    Combines FileStore (current configs) and VersionStore (snapshots)
    to provide versioned mapping configuration storage.

    Each mapping is identified by a content_hash (SHA1 of the source CSV file).

    Example:
        ```python
        repo = MappingRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Store a mapping configuration
        repo.put(mapping)

        # Get a mapping configuration
        mapping = repo.get(content_hash)

        # List all mappings
        for content_hash in repo.list():
            print(content_hash)
        ```
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        self._versions = VersionedModelStore(uri, DatasetMapping)

    def exists(self, content_hash: str) -> bool:
        """Check if a mapping configuration exists."""
        mapping_path = path.mapping(content_hash)
        return self._versions.exists(mapping_path)

    def get(self, content_hash: str) -> DatasetMapping:
        """
        Get a mapping configuration by content hash.

        Args:
            content_hash: SHA1 checksum of the source CSV file

        Returns:
            DatasetMapping if exists, None otherwise
        """
        mapping_path = path.mapping(content_hash)
        return self._versions.get(mapping_path)

    def put(self, mapping: DatasetMapping) -> str:
        """
        Store a mapping configuration.

        Creates both a current config and a versioned snapshot.

        Args:
            mapping: The mapping configuration to store

        Returns:
            The current version path
        """
        content_hash = mapping.content_hash
        mapping_path = path.mapping(content_hash)
        key = self._versions.make(mapping_path, mapping)

        self.log.info(
            f"Stored mapping `{mapping_path}`",
            content_hash=content_hash,
            version=key,
        )

        return key

    def delete(self, content_hash: str) -> None:
        """Delete a mapping configuration."""
        mapping_path = path.mapping(content_hash)
        self._versions.delete(mapping_path)
        self.log.warning("Deleted mapping", content_hash=content_hash)

    def list(self) -> Generator[str, None, None]:
        """
        List all content hashes that have mapping configurations.

        Yields:
            Content hash strings for files with mapping.yml configs
        """
        # Glob matches only direct mapping.yml, not versioned copies
        for key in self._versions.iterate_keys(
            prefix=path.MAPPINGS, glob=f"*/{path.MAPPING}"
        ):
            # Keys look like: mappings/<content_hash>/mapping.yml
            parts = key.split("/")
            if len(parts) == 3:
                yield parts[1]  # content_hash

    def iterate(self) -> Generator[DatasetMapping, None, None]:
        """Iterate all mapping configurations."""
        for content_hash in self.list():
            yield self.get(content_hash)
