"""Layer 4: Multi-step workflow operations.

Operations coordinate across repositories for complex workflows.
They are internal and triggered by the Dataset class.

Factory functions provide convenient ways to run operations from a Dataset:

    from ftm_lakehouse import get_dataset
    from ftm_lakehouse.operation import export, make, optimize

    dataset = get_dataset("my_dataset")
    export(dataset, "statements")  # Run single export
    optimize(dataset)              # Merge + compact + vacuum
    make(dataset)                  # Run full workflow
"""

from ftm_lakehouse.operation.crawl import CrawlOperation, crawl
from ftm_lakehouse.operation.download import DownloadArchiveOperation
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
from ftm_lakehouse.operation.factories import (
    download_archive,
    export,
    make,
    optimize,
    run_mapping,
)
from ftm_lakehouse.operation.maintenance import OptimizeJob, OptimizeOperation
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingOperation

__all__ = [
    # Operations
    "CrawlOperation",
    "DownloadArchiveOperation",
    "ExportJob",
    "ExportKind",
    "ExportOperation",
    "MakeJob",
    "MakeOperation",
    "MappingOperation",
    "OptimizeJob",
    "OptimizeOperation",
    # Factory functions
    "crawl",
    "download_archive",
    "export",
    "make",
    "optimize",
    "run_mapping",
]
