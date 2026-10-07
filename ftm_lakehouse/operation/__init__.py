"""Layer 4: multi-step workflows across repositories, run through the factories:

from ftm_lakehouse.operation import export, make, optimize

optimize("my_dataset")  # merge + vacuum
export("my_dataset")    # every export artifact from one sweep
make("my_dataset")      # flush the journal, then export
"""

from ftm_lakehouse.operation.crawl import CrawlOperation, crawl
from ftm_lakehouse.operation.download import DownloadArchiveOperation
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
from ftm_lakehouse.operation.factories import (
    download_archive,
    export,
    make,
    migrate,
    optimize,
    shard,
)
from ftm_lakehouse.operation.maintenance import (
    MigrateJob,
    MigrateOperation,
    OptimizeJob,
    OptimizeOperation,
    ShardJob,
    ShardOperation,
)
from ftm_lakehouse.operation.make import MakeJob, MakeOperation

__all__ = [
    # Operations
    "CrawlOperation",
    "DownloadArchiveOperation",
    "ExportJob",
    "ExportKind",
    "ExportOperation",
    "MakeJob",
    "MakeOperation",
    "MigrateJob",
    "MigrateOperation",
    "OptimizeJob",
    "OptimizeOperation",
    "ShardJob",
    "ShardOperation",
    # Factory functions
    "crawl",
    "download_archive",
    "export",
    "make",
    "migrate",
    "optimize",
    "shard",
]
