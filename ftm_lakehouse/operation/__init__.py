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
    compact,
    download_archive,
    export_documents,
    export_entities,
    export_index,
    export_statements,
    export_statistics,
    make,
    merge,
    recreate,
    run_mapping,
    vacuum,
)
from ftm_lakehouse.operation.maintenance import (
    CompactOperation,
    MergeOperation,
    VacuumOperation,
)
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingOperation

__all__ = [
    # Operations
    "CompactOperation",
    "CrawlOperation",
    "DownloadArchiveOperation",
    "ExportDocumentsOperation",
    "ExportEntitiesOperation",
    "ExportIndexOperation",
    "ExportStatementsOperation",
    "ExportStatisticsOperation",
    "MakeJob",
    "MakeOperation",
    "MappingOperation",
    "MergeOperation",
    "VacuumOperation",
    # Factory functions
    "compact",
    "crawl",
    "download_archive",
    "export_documents",
    "export_entities",
    "export_index",
    "export_statements",
    "export_statistics",
    "make",
    "merge",
    "recreate",
    "run_mapping",
    "vacuum",
]
