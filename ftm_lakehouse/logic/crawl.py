"""Crawl logic for document collections from local folders or remote sources.

This module provides the crawling infrastructure for importing documents from
local or remote file stores into the lakehouse. This just adds (or replaces)
documents but no processing. Use `ingest-file` or any other client for that.
"""

from datetime import datetime
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Generator

import aiohttp
from anystore import get_store
from anystore.store import BaseStore
from anystore.types import Uri
from anystore.util import make_uri_key
from banal import ensure_dict

from ftm_lakehouse.core.decorators import storage_cache
from ftm_lakehouse.core.mixins import LakeMixin
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.service import JobRun

if TYPE_CHECKING:
    from ftm_lakehouse.lake import DatasetLakehouse


CRAWL_ORIGIN = "crawl"
"""Default origin identifier for crawled files."""


def make_cache_key(worker: "CrawlWorker", uri: str, *args, **kwargs) -> str | None:
    """
    Generate cache key for crawl tasks based on URI.

    Used by the storage_cache decorator to determine if a file has
    already been crawled.

    Args:
        worker: The CrawlWorker instance
        uri: The URI of the file being crawled

    Returns:
        Cache key string if caching is enabled, None otherwise
    """
    if not worker.job.skip_existing:
        return None
    if worker.job.cache_key_uri:
        return f"crawl/{make_uri_key(uri)}"
    return None
    # FIXME create other key logic


class CrawlJob(DatasetJobModel):
    """
    Job model for crawl operations.

    Tracks the state and configuration of a crawl job, including
    progress counters and filtering options.

    Attributes:
        uri: Source location URI to crawl
        skip_existing: Skip files that have already been crawled
        cache_key_uri: Use URI (not checksum) as cache key
        prefix: Include only keys with this prefix
        exclude_prefix: Exclude keys with this prefix
        glob: Include only keys matching this glob pattern
        exclude_glob: Exclude keys matching this glob pattern
    """

    uri: Uri | None = None
    skip_existing: bool | None = True
    cache_key_uri: bool | None = True
    prefix: str | None = None
    exclude_prefix: str | None = None
    glob: str | None = None
    exclude_glob: str | None = None


class CrawlWorker(LakeMixin):
    """
    Worker that processes crawl tasks.

    Iterates through files in a source store, archives them, and
    creates corresponding entities in the dataset.

    Args:
        dataset: Target dataset to crawl into
        job: CrawlJob configuration
        source: Source store to crawl from

    Example:
        ```python
        from ftm_lakehouse.logic.crawl import CrawlWorker, CrawlJob
        from anystore import get_store

        job = CrawlJob.make(
            uri="s3://bucket/documents",
            dataset="my_dataset",
            glob="*.pdf"
        )
        source = get_store(job.uri)
        worker = CrawlWorker(dataset, job, source)
        result = worker.run()
        print(f"Crawled {result.done} files")
        ```
    """

    def __init__(
        self, dataset: "DatasetLakehouse", job: CrawlJob, source: BaseStore
    ) -> None:
        super().__init__(dataset.name, dataset.uri)
        self.dataset = dataset
        self.job = job
        self.log = job.log
        self.source = source

    def get_tasks(self) -> Generator[str, None, None]:
        """
        Generate tasks (file keys) to crawl.

        Applies prefix, glob, and exclude filters to the source store's
        file listing.

        Yields:
            File keys to be crawled
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

    @storage_cache(key_func=make_cache_key)
    def handle_task(self, task: str, run: JobRun) -> datetime:
        """
        Handle a single crawl task.

        Archives the file and creates a corresponding entity.

        Args:
            task: File key to crawl
            run: Current job run context

        Returns:
            Timestamp when the task was processed
        """
        now = datetime.now()
        self.log.info(f"Crawling `{task}` ...", source=self.source.uri)
        file = self.dataset.archive.archive_file(task, self.source, origin=CRAWL_ORIGIN)
        self.dataset.entities.add(file.to_entity(), CRAWL_ORIGIN)
        run.job.done += 1
        return now

    def run(self) -> CrawlJob:
        """
        Execute the crawl job.

        Processes all tasks and returns the completed job with statistics.

        Returns:
            CrawlJob with final statistics (done, pending counts)

        Raises:
            RuntimeError: If job result is None
        """
        with self.dataset.jobs.run(self.job) as run:
            for ix, task in enumerate(self.get_tasks(), 1):
                if ix % 1000 == 0:
                    self.log.info(
                        f"Handling task {ix} ...",
                        pending=self.job.pending,
                        done=self.job.done,
                    )
                    run.save()
                self.handle_task(task, run)
                run.job.pending -= 1
                run.job.touch()

        result = run.jobs.latest(CrawlJob)
        if result is not None:
            return result
        raise RuntimeError("Result is `None`")


def crawl(
    uri: Uri,
    dataset: "DatasetLakehouse",
    skip_existing: bool | None = True,
    cache_key_uri: bool | None = True,
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    glob: str | None = None,
    exclude_glob: str | None = None,
) -> CrawlJob:
    """
    Crawl a local or remote location of documents into a dataset.

    This is the main entry point for crawling documents. It handles
    store configuration, job creation, and worker execution.

    Supports local filesystem, S3, HTTP, and other storage backends
    supported by anystore/fsspec.

    Args:
        uri: Source location URI (local path, s3://, http://, etc.)
        dataset: Target dataset to crawl into
        skip_existing: Don't re-crawl files that are already cached
        cache_key_uri: Use URI (not content hash) as cache key
        prefix: Include only keys with this prefix (e.g., "docs/")
        exclude_prefix: Exclude keys with this prefix
        glob: Glob pattern for keys to include (e.g., "**/*.pdf")
        exclude_glob: Glob pattern for keys to exclude

    Returns:
        CrawlJob with completion statistics

    Example:
        ```python
        from ftm_lakehouse import get_dataset
        from ftm_lakehouse.logic import crawl

        dataset = get_dataset("my_dataset")

        # Crawl local directory
        result = crawl("/path/to/documents", dataset, glob="*.pdf")
        print(f"Crawled {result.done} files")

        # Crawl S3 bucket
        result = crawl(
            "s3://my-bucket/docs",
            dataset,
            prefix="2024/",
            exclude_glob="*.tmp"
        )
        ```
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
        dataset=dataset.name,
        skip_existing=skip_existing,
        cache_key_uri=cache_key_uri,
        prefix=prefix,
        exclude_prefix=exclude_prefix,
        glob=glob,
        exclude_glob=exclude_glob,
    )

    worker = CrawlWorker(dataset, job, store)
    return worker.run()
