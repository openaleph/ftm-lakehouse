from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, Generator, Literal, Self, TypeAlias

from anystore.logging import BoundLogger, get_logger
from anystore.model import BaseModel, StoreModel
from anystore.store.base import Stats
from anystore.types import SDict
from anystore.util import ensure_uuid, make_data_checksum
from followthemoney import EntityProxy, StatementEntity
from followthemoney.dataset import DefaultDataset
from ftmq.model import Catalog, Dataset
from ftmq.types import StatementEntities
from ftmq.util import make_entity
from pydantic import field_validator

from ftm_lakehouse.conventions import path
from ftm_lakehouse.util import mime_to_schema

CrudOperation: TypeAlias = Literal["create", "update", "delete"]


DEFAULT_USER = "__system__"


class Crud(BaseModel):
    """Operations triggered by humans"""

    uuid: str
    """unique identifier"""
    dataset: str
    """Dataset name (foreign_id)"""
    operation: CrudOperation = "update"
    """the action to perform"""
    entity: SDict
    """Entity payload"""
    user: str = DEFAULT_USER
    """User identifier"""
    timestamp: datetime
    """Timestamp"""

    @field_validator("uuid", mode="before")
    @classmethod
    def ensure_uuid(cls, value: Any) -> Any:
        return ensure_uuid(value)

    @property
    def storage_path(self) -> str:
        """Relative path in dataset lakehouse"""
        return path.crud_path(self.entity["id"], str(self.uuid))

    @property
    def entity_id(self) -> str:
        return self.entity["id"]

    def as_entity(self) -> StatementEntity:
        return make_entity(
            self.entity, entity_type=StatementEntity, default_dataset=self.dataset
        )

    @classmethod
    def make(
        cls,
        dataset: str,
        entity: EntityProxy,
        user: str | None = DEFAULT_USER,
        operation: CrudOperation | None = "update",
    ) -> Self:
        return cls(
            uuid=ensure_uuid(),
            dataset=dataset,
            entity=entity.to_dict(),
            user=user or DEFAULT_USER,
            operation=operation or "update",
            timestamp=datetime.now(),
        )


Cruds: TypeAlias = Generator[Crud, None, None]


class CatalogModel(Catalog):
    storage: StoreModel | None = None
    """Lakehouse storage base path"""
    public_url_prefix: str | None = None
    """Rewrite public archive urls"""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""


class File(Stats):
    dataset: str
    checksum: str
    extra: dict[str, Any] = {}

    def to_entity(self) -> StatementEntity:
        entity = make_entity(
            {"id": self.id, "schema": mime_to_schema(self.mimetype)},
            entity_type=StatementEntity,
            default_dataset=self.dataset,
        )
        entity.add("contentHash", self.checksum)
        entity.add("fileName", self.name)
        entity.add("fileSize", self.size)
        entity.add("mimeType", self.mimetype)
        return entity

    def make_parents(self) -> StatementEntities:
        raise NotImplementedError

    # def make_folders(path: str, collection_id: str, parent: str | None = None) -> str:
    #     api = get_api()
    #     log.info(f"Creating folder: `{path}`", host=get_host(api))
    #     folder = Path(path)
    #     foreign_id = "/".join(folder.parts)  # same as alephclient
    #     if len(folder.parts) > 1:
    #         parent = make_folders(os.path.join(*folder.parts[:-1]), collection_id, parent)
    #     metadata: dict[str, Any] = {"file_name": folder.name, "foreign_id": foreign_id}
    #     if parent is not None:
    #         metadata["parent"] = {"id": parent}
    #     res = api.ingest_upload(collection_id, metadata=metadata)
    #     return res["id"]

    @property
    def id(self) -> str:
        return f"file-{make_data_checksum((self.key, self.checksum))}"

    @property
    def archive_path(self) -> str:
        """Relative path in dataset archive"""
        return path.file_path(self.checksum)

    @property
    def archive_path_meta(self) -> str:
        """Relative path for metadata json in dataset archive"""
        return path.file_path_meta(self.checksum)

    @classmethod
    def from_info(cls, info: Stats, checksum: str, **data) -> Self:
        data["dataset"] = data.get("dataset", DefaultDataset.name)
        data["checksum"] = checksum
        return cls(**{**info.model_dump(), **data})

    def to_dict(self) -> SDict:
        return self.model_dump(
            mode="json",
            include={
                "dataset",
                "checksum",
                "key",
                "size",
                "mimetype",
                "created_at",
                "updated_at",
            },
        )


Files: TypeAlias = Generator[File, None, None]


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

    @property
    def name(self) -> str:
        return self.get_name()

    @classmethod
    def get_name(cls) -> str:
        return cls.__name__

    @field_validator("run_id", mode="before")
    @classmethod
    def ensure_run_id(cls, value: str | None = None) -> str:
        """Give a manual run id or create one"""
        return value or ensure_uuid()

    def touch(self) -> None:
        self.last_updated = datetime.now()

    def stop(self, exc: Exception | None = None) -> None:
        self.running = False
        self.stopped = datetime.now()
        self.exc = str(exc)
        if self.started and self.stopped:
            self.took = self.stopped - self.started

    @classmethod
    def make(cls, **kwargs) -> Self:
        kwargs["run_id"] = cls.ensure_run_id(kwargs.get("run_id"))
        return cls(**kwargs)

    @classmethod
    def start(cls, **kwargs) -> Self:
        run = cls.make(**kwargs)
        run.started = datetime.now()
        run.running = True
        run.touch()
        return run

    @cached_property
    def log(self, **ctx) -> BoundLogger:
        return get_logger(__name__, run_id=self.run_id, **ctx)


class DatasetJobModel(JobModel):
    dataset: str

    @cached_property
    def log(self, **ctx) -> BoundLogger:
        return get_logger(
            f"{__name__}.{self.dataset}",
            run_id=self.run_id,
            dataset=self.dataset,
            **ctx,
        )
