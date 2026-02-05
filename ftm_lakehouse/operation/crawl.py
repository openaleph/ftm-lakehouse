"""CrawlOperation - source → files → entities workflow.

This module provides the crawling infrastructure for importing documents from
local or remote file stores into the lakehouse. This just adds (or replaces)
documents but no processing. Use `ingest-file` or any other client for that.
"""

from datetime import datetime
from enum import Enum
from fnmatch import fnmatch
from typing import Generator

import aiohttp
from anystore.store import get_store
from anystore.types import Uri
from banal import ensure_dict

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository import ArchiveRepository, EntityRepository, JobRepository
from ftm_lakehouse.repository.job import JobRun


class HandleExistingMode(str, Enum):
    overwrite = "overwrite"
    skip_path = "skip_path"
    skip_checksum = "skip_checksum"


class CrawlJob(DatasetJobModel):
    """
    Job model for crawl operations.

    Tracks the state and configuration of a crawl job.

    Attributes:
        uri: Source location URI to crawl
        prefix: Include only keys with this prefix
        exclude_prefix: Exclude keys with this prefix
        glob: Include only keys matching this glob pattern
        exclude_glob: Exclude keys matching this glob pattern
    """

    uri: Uri
    prefix: str | None = None
    exclude_prefix: str | None = None
    glob: str | None = None
    exclude_glob: str | None = None
    make_entities: bool = False
    existing: HandleExistingMode | None = HandleExistingMode.skip_path


class CrawlOperation(DatasetJobOperation[CrawlJob]):
    """
    Crawl workflow that archives files and creates entities.

    Iterates through files in a source store, archives them to the
    file repository, and creates corresponding entities in the
    entities repository.

    Example:
        ```python
        from ftm_lakehouse.operation import CrawlOperation, CrawlJob

        job = CrawlJob.make(
            uri="s3://bucket/documents",
            dataset="my_dataset",
            glob="*.pdf"
        )
        op = CrawlOperation(job=job)
        result = op.run()
        print(f"Crawled {result.done} files")
        ```
    """

    target = tag.OP_CRAWL

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source = get_store(self.job.uri)

    def get_uris(self) -> Generator[str, None, None]:
        """
        Generate file uris to crawl.

        Applies prefix, glob, and exclude filters to the source store.

        Yields:
            File uris to be crawled
        """
        self.log.info(f"Crawling `{self.job.uri}` ...")
        for key in self.source.iterate_keys(
            prefix=self.job.prefix,
            exclude_prefix=self.job.exclude_prefix,
            glob=self.job.glob,
        ):
            if self.job.exclude_glob and fnmatch(key, self.job.exclude_glob):
                continue
            self.job.pending += 1
            self.job.touch()
            yield key

    def handle_crawl(self, uri: str, run: JobRun[CrawlJob]) -> datetime:
        """
        Handle a single crawl task.

        Archives the file and creates a corresponding entity.

        Args:
            uri: File uri to crawl
            run: Current job run context

        Returns:
            Timestamp when the task was processed
        """
        now = datetime.now()

        self.log.info(f"Crawling `{uri}` ...", source=self.source.uri)
        checksum = None
        if self.source.is_local:
            checksum = self.source.checksum(uri)
        if not self._should_skip(uri, checksum):
            file = self.archive.store(
                self.source.to_uri(uri),
                checksum=checksum,
                key=uri,
                origin=tag.CRAWL_ORIGIN,
            )
            if self.job.make_entities:
                self.entities.add_many(file.make_entities(), tag.CRAWL_ORIGIN)
            run.job.done += 1
        return now

    def handle(self, run: JobRun, *args, **kwargs) -> None:
        for ix, task in enumerate(self.get_uris(), 1):
            if ix % 1000 == 0:
                self.log.info(
                    f"Handling task {ix} ...",
                    pending=self.job.pending,
                    done=self.job.done,
                )
                run.save()
            self.handle_crawl(task, run)
            run.job.pending -= 1
            run.job.touch()
        if self.job.make_entities:
            self.entities.flush()

    def _should_skip(self, uri: Uri, checksum: str | None) -> bool:
        if self.job.existing is None:
            return False
        if self.job.existing == HandleExistingMode.overwrite:
            return False
        if checksum is None:
            return False
        if self.job.existing == HandleExistingMode.skip_checksum:
            return self.archive.exists(checksum)
        if self.job.existing == HandleExistingMode.skip_path:
            if self.archive.exists(checksum):
                for file in self.archive.get_all_files(checksum):
                    if file.key == str(uri):
                        return True
        return False


def crawl(
    dataset: str,
    uri: Uri,
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    glob: str | None = None,
    exclude_glob: str | None = None,
    make_entities: bool | None = False,
    existing: HandleExistingMode | None = HandleExistingMode.skip_path,
    archive: ArchiveRepository | None = None,
    entities: EntityRepository | None = None,
    jobs: JobRepository | None = None,
) -> CrawlJob:
    """
    Crawl a local or remote location of documents.

    This is the main entry point for crawling documents.

    Args:
        uri: Source location URI (local path, s3://, http://, etc.)
        prefix: Include only keys with this prefix
        exclude_prefix: Exclude keys with this prefix
        glob: Glob pattern for keys to include
        exclude_glob: Glob pattern for keys to exclude
        make_entities: Create file entities from crawled files
        existing: Ignore already existing (by relative path, checksum) or overwrite
        archive: ArchiveRepository for blobs
        entities: EntityRepository for entities
        jobs: JobRepository for job tracking

    Returns:
        CrawlJob with completion statistics
    """
    store = get_store(uri=uri)
    if store.is_http:
        backend_config: dict = ensure_dict(store.backend_config)
        backend_config["client_kwargs"] = {
            **ensure_dict(backend_config.get("client_kwargs")),
            "timeout": aiohttp.ClientTimeout(total=3600 * 24),
        }
        store.backend_config = backend_config

    job = CrawlJob.make(
        uri=store.uri,
        dataset=dataset,
        prefix=prefix,
        exclude_prefix=exclude_prefix,
        glob=glob,
        exclude_glob=exclude_glob,
        make_entities=make_entities,
        existing=existing,
    )

    op = CrawlOperation(
        job=job,
        archive=archive,
        entities=entities,
        jobs=jobs,
    )
    return op.run()
