"""Operation API routes: execute DatasetJob as operation"""

import asyncio

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ftm_lakehouse.api.dependencies import Dataset
from ftm_lakehouse.model import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.operation.crawl import CrawlJob, CrawlOperation
from ftm_lakehouse.operation.download import (
    DownloadArchiveJob,
    DownloadArchiveOperation,
)
from ftm_lakehouse.operation.export import (
    ExportDocumentsJob,
    ExportDocumentsOperation,
    ExportEntitiesJob,
    ExportEntitiesOperation,
    ExportIndexJob,
    ExportIndexOperation,
    ExportStatementsJob,
    ExportStatementsOperation,
    ExportStatisticsJob,
    ExportStatisticsOperation,
)
from ftm_lakehouse.operation.maintenance import (
    CompactJob,
    CompactOperation,
    MergeJob,
    MergeOperation,
    VacuumJob,
    VacuumOperation,
)
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.operation.recreate import RecreateJob, RecreateOperation

router = APIRouter()


OPERATIONS: dict[str, tuple[type[DatasetJobModel], type[DatasetJobOperation]]] = {
    "CrawlJob": (CrawlJob, CrawlOperation),
    "CompactJob": (CompactJob, CompactOperation),
    "MergeJob": (MergeJob, MergeOperation),
    "VacuumJob": (VacuumJob, VacuumOperation),
    "ExportStatementsJob": (ExportStatementsJob, ExportStatementsOperation),
    "ExportEntitiesJob": (ExportEntitiesJob, ExportEntitiesOperation),
    "ExportStatisticsJob": (ExportStatisticsJob, ExportStatisticsOperation),
    "ExportDocumentsJob": (ExportDocumentsJob, ExportDocumentsOperation),
    "ExportIndexJob": (ExportIndexJob, ExportIndexOperation),
    "MappingJob": (MappingJob, MappingOperation),
    "RecreateJob": (RecreateJob, RecreateOperation),
    "DownloadArchiveJob": (DownloadArchiveJob, DownloadArchiveOperation),
    "MakeJob": (MakeJob, MakeOperation),
}


@router.post("/{dataset}/_api/operations")
async def run_operation(
    dataset: Dataset, request: Request, force: bool = False
) -> JSONResponse:
    """Run a job operation on the given dataset.

    The request body must be a serialized DatasetJobModel with a `name` field
    identifying the job type (e.g. "CompactJob", "CrawlJob").
    """
    body = await request.json()
    name = body.pop("name", None)
    body.pop("dataset", None)  # Use dataset from URL, not body

    if name not in OPERATIONS:
        raise HTTPException(status_code=400, detail=f"Unknown operation: `{name}`")

    model_cls, op_cls = OPERATIONS[name]
    job = model_cls.make(dataset=dataset.name, **body)
    op = op_cls.from_job(job, dataset)
    result = await asyncio.to_thread(op.run, force=force)
    return JSONResponse(result.model_dump(mode="json"))
