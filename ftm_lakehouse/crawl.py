"""
Crawl document collections from local folders or remote sources
"""

from datetime import datetime
from fnmatch import fnmatch
from typing import Generator

import aiohttp
from anystore import get_store
from anystore.store import BaseStore
from anystore.types import Uri
from anystore.util import make_uri_key
from banal import ensure_dict
from ftmq.store.lake import LakeWriter

from ftm_lakehouse.decorators import storage_cache
from ftm_lakehouse.lake.base import DatasetLakehouse
from ftm_lakehouse.lake.mixins import LakeMixin
from ftm_lakehouse.model import DatasetJobModel


def make_cache_key(worker: "CrawlWorker", uri: str, *args, **kwargs) -> str | None:
    if worker.job.cache_key_uri:
        return f"crawl/{make_uri_key(uri)}"
    # FIXME
    # generate other key, based on http header or other criteria


class CrawlJob(DatasetJobModel):
    uri: Uri | None = None
    skip_existing: bool | None = True
    cache_key_uri: bool | None = True
    prefix: str | None = None
    exclude_prefix: str | None = None
    glob: str | None = None
    exclude_glob: str | None = None


class CrawlWorker(LakeMixin):
    def __init__(
        self, dataset: DatasetLakehouse, job: CrawlJob, source: BaseStore
    ) -> None:
        super().__init__(dataset.name, dataset.uri)
        self.dataset = dataset
        self.job = job
        self.log = job.log
        self.source = source

    def get_tasks(self) -> Generator[str, None, None]:
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
    def handle_task(self, task: str, writer: LakeWriter) -> datetime:
        now = datetime.now()
        self.log.info(f"Crawling `{task}` ...", source=self.source.uri)
        file = self.dataset.archive.archive_file(task, self.source)
        writer.add_entity(file.to_entity())
        return now

    def run(self) -> CrawlJob:
        with self.dataset.jobs.run(self.job) as run:
            with self.dataset.statements.bulk(origin="crawl") as bulk:
                for ix, task in enumerate(self.get_tasks(), 1):
                    if ix % 1000 == 0:
                        self.log.info(
                            f"Handling task {ix} ...",
                            pending=self.job.pending,
                            done=self.job.done,
                        )
                        run.save()
                    self.handle_task(task, bulk)
                    run.job.pending -= 1
                    run.job.done += 1
                    run.job.touch()
        result = run.jobs.latest(CrawlJob)
        if result is not None:
            return result
        raise RuntimeError("Result is `None`")


def crawl(
    uri: Uri,
    dataset: DatasetLakehouse,
    skip_existing: bool | None = True,
    cache_key_uri: bool | None = True,
    prefix: str | None = None,
    exclude_prefix: str | None = None,
    glob: str | None = None,
    exclude_glob: str | None = None,
) -> CrawlJob:
    """
    Crawl a local or remote location of documents into a ftm_lakehouse dataset.

    Args:
        uri: local or remote location uri that supports file listing
        dataset: Dataset instance
        skip_existing: Don't re-crawl existing files (by checksum or uri)
        cache_key_uri: Use uri (not checksum) as cache key to detect already
            crawled files
        prefix: Include only keys with the given prefix (e.g. "foo/bar")
        exclude_prefix: Exclude keys with this prefix
        glob: Path-style glob pattern for keys to filter (e.g. "foo/**/*.json")
        exclude_glob: Path-style glob pattern for keys to exclude (e.g.
            "foo/**/*.json")
    """
    store = get_store(uri=uri)
    # FIXME ensure long timeouts for http sources
    if store.is_http:
        backend_config = ensure_dict(store.backend_config)
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
