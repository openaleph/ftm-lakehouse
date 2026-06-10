"""The API operations registry is derived from the operation package exports."""

from ftm_lakehouse.api.routes.operations import OPERATIONS
from ftm_lakehouse.operation.crawl import CrawlJob, CrawlOperation
from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.operation.maintenance import OptimizeJob, OptimizeOperation
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation


def test_operations_registry_derived():
    """Pins the public job-name contract of ``POST /_api/operations``.

    A new operation becomes routable by exporting it from
    ``ftm_lakehouse.operation``; this assertion is the place that
    documents the resulting contract change.
    """
    assert OPERATIONS == {
        "CrawlJob": (CrawlJob, CrawlOperation),
        "DownloadArchiveJob": (DownloadArchiveJob, DownloadArchiveOperation),
        "ExportJob": (ExportJob, ExportOperation),
        "MakeJob": (MakeJob, MakeOperation),
        "MappingJob": (MappingJob, MappingOperation),
        "OptimizeJob": (OptimizeJob, OptimizeOperation),
    }
