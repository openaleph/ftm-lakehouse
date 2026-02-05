"""Layer 4: Multi-step workflow operations.

Operations coordinate across repositories for complex workflows.
They are internal and triggered by the Dataset class.

Factory functions provide convenient ways to run operations from a Dataset:

    from ftm_lakehouse import get_dataset
    from ftm_lakehouse.operation import export_statements, make

    dataset = get_dataset("my_dataset")
    export_statements(dataset)  # Run single export
    make(dataset)               # Run full workflow
"""

from ftm_lakehouse.operation.crawl import CrawlOperation, crawl
from ftm_lakehouse.operation.download import DownloadArchiveOperation
from ftm_lakehouse.operation.export import (
    ExportDocumentsOperation,
    ExportEntitiesOperation,
    ExportIndexOperation,
    ExportStatementsOperation,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.factories import (
    download_archive,
    export_documents,
    export_entities,
    export_index,
    export_statements,
    export_statistics,
    make,
    optimize,
    recreate,
    run_mapping,
)
from ftm_lakehouse.operation.mapping import MappingOperation
from ftm_lakehouse.operation.optimize import OptimizeOperation

__all__ = [
    # Operations
    "CrawlOperation",
    "DownloadArchiveOperation",
    "ExportDocumentsOperation",
    "ExportEntitiesOperation",
    "ExportIndexOperation",
    "ExportStatementsOperation",
    "ExportStatisticsOperation",
    "MappingOperation",
    "OptimizeOperation",
    # Factory functions
    "crawl",
    "download_archive",
    "export_documents",
    "export_entities",
    "export_index",
    "export_statements",
    "export_statistics",
    "make",
    "optimize",
    "recreate",
    "run_mapping",
]
