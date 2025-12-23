"""MappingOperation - config → entities → journal workflow."""

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.logic.mappings import map_entities
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.model.mapping import mapping_origin
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository import MappingRepository
from ftm_lakehouse.repository.job import JobRun


class MappingJob(DatasetJobModel):
    content_hash: str
    entities: int = 0


class MappingOperation(DatasetJobOperation[MappingJob]):
    """
    Mapping workflow that transforms a CSV file into entities.

    Processes a single archived CSV file (identified by content_hash)
    using its mapping configuration to generate FollowTheMoney entities,
    which are written to the entity repository.

    Example:
        ```python
        from ftm_lakehouse.operation import MappingOperation, MappingJob

        job = MappingJob.make(
            dataset="my_dataset",
            content_hash="5a6acf229ba576d9a40b09292595658bbb74ef56",
        )
        op = MappingOperation(job=job)
        result = op.run()
        print(f"Generated {result.done} entities")
        ```
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.mappings = MappingRepository(self.job.dataset, self.archive.uri)

    def get_target(self) -> str:
        return tag.mapping_tag(self.job.content_hash)

    def get_dependencies(self) -> list[str]:
        return [path.mapping(self.job.content_hash)]

    def handle(self, run: JobRun[MappingJob], *args, **kwargs) -> None:
        """
        Process the mapping configuration and store generated entities.

        Skips processing if the mapping output is already up-to-date
        relative to the mapping config.
        """
        origin = mapping_origin(self.job.content_hash)
        mapping = self.mappings.get(self.job.content_hash)
        file = self.archive.get(self.job.content_hash)
        with self.archive.local_path(file) as csv_path:
            with self.entities.bulk(origin=origin) as bulk:
                for entity in map_entities(mapping, csv_path):
                    bulk.add_entity(entity)
                    run.job.done += 1
        self.entities.flush()
