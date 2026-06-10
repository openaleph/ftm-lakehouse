"""Operation API routes: execute DatasetJob as operation"""

import asyncio
from typing import get_args

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ftm_lakehouse import operation as operation_module
from ftm_lakehouse.api.dependencies import Dataset
from ftm_lakehouse.model import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation

router = APIRouter()


def _registry() -> dict[str, tuple[type[DatasetJobModel], type[DatasetJobOperation]]]:
    """Pair every operation exported by :mod:`ftm_lakehouse.operation` with
    the job model from its ``DatasetJobOperation[...]`` generic parameter,
    keyed by the job-model class name (the ``name`` field of a posted job).

    Derived instead of hand-maintained so a new operation only needs to be
    exported from the operation package to become routable here.
    """
    registry: dict[str, tuple[type[DatasetJobModel], type[DatasetJobOperation]]] = {}
    for obj in vars(operation_module).values():
        if not isinstance(obj, type) or not issubclass(obj, DatasetJobOperation):
            continue
        for base in getattr(obj, "__orig_bases__", ()):
            args = get_args(base)
            if (
                args
                and isinstance(args[0], type)
                and issubclass(args[0], DatasetJobModel)
            ):
                registry[args[0].__name__] = (args[0], obj)
    return registry


OPERATIONS = _registry()


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
    op = op_cls.from_job(job, dataset)
    result = await asyncio.to_thread(op.run, force=force)
    return JSONResponse(result.model_dump(mode="json"))
