"""Operation API routes: execute DatasetJob as operation"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ftm_lakehouse.api.helpers import Dataset
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
from ftm_lakehouse.operation.make import MakeJob, MakeOperation
from ftm_lakehouse.operation.mapping import MappingJob, MappingOperation
from ftm_lakehouse.operation.optimize import OptimizeJob, OptimizeOperation
from ftm_lakehouse.operation.recreate import RecreateJob, RecreateOperation

router = APIRouter()


OPERATIONS: dict[str, tuple[type[DatasetJobModel], type[DatasetJobOperation]]] = {
    "CrawlJob": (CrawlJob, CrawlOperation),
    "OptimizeJob": (OptimizeJob, OptimizeOperation),
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
    identifying the job type (e.g. "OptimizeJob", "CrawlJob").
    """
    body = await request.json()
    name = body.pop("name", None)
    body.pop("dataset", None)  # Use dataset from URL, not body

    if name not in OPERATIONS:
        raise HTTPException(status_code=400, detail=f"Unknown operation: `{name}`")

    model_cls, op_cls = OPERATIONS[name]
    job = model_cls.make(dataset=dataset.name, **body)
    result = op_cls.from_job(job, dataset).run(force=force)
    return JSONResponse(result.model_dump(mode="json"))
