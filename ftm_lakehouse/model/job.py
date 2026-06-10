"""Job status models."""

from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Self, TypeVar

from anystore.logging import get_logger
from anystore.model import BaseModel
from anystore.util import ensure_uuid
from pydantic import computed_field, field_validator
from structlog.stdlib import BoundLogger

DEFAULT_USER = "__system__"


J = TypeVar("J", bound="JobModel")
DJ = TypeVar("DJ", bound="DatasetJobModel")


class JobModel(BaseModel):
    """Status model for a (probably long running) job"""

    run_id: str
    started: datetime | None = None
    stopped: datetime | None = None
    last_updated: datetime | None = None
    pending: int = 0
    done: int = 0
    errors: int = 0
    running: bool = False
    exc: str | None = None
    took: timedelta = timedelta()

    @computed_field
    @property
    def name(self) -> str:
        return self.__class__.__name__

    @field_validator("run_id", mode="before")
    @classmethod
    def ensure_run_id(cls, value: str | None = None) -> str:
        """Give a manual run id or create one"""
        return value or ensure_uuid()

    def touch(self) -> None:
        self.last_updated = datetime.now(timezone.utc)

    def stop(self, exc: Exception | None = None) -> None:
        self.running = False
        self.stopped = datetime.now(timezone.utc)
        self.exc = str(exc)
        if self.started and self.stopped:
            self.took = self.stopped - self.started

    @classmethod
    def make(cls, **kwargs) -> Self:
        kwargs["run_id"] = cls.ensure_run_id(kwargs.get("run_id"))
        return cls(**kwargs)

    @cached_property
    def log(self) -> BoundLogger:
        return get_logger(__name__, run_id=self.run_id)


class DatasetJobModel(JobModel):
    """Status model for a (probably long running) job bound to a dataset"""

    dataset: str

    @cached_property
    def log(self) -> BoundLogger:
        return get_logger(
            f"{self.dataset}.{self.name}",
            run_id=self.run_id,
            dataset=self.dataset,
        )
