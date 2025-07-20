from datetime import datetime
from pathlib import Path

from ftm_lakehouse.conventions import path
from ftm_lakehouse.lake.mixins import LakeMixin
from ftm_lakehouse.model import Crud, CrudOperation, Cruds


def _done(uid: str) -> str:
    return f"cruds/done/{uid}"


class DatasetCruds(LakeMixin):
    def iterate(
        self,
        entity_id: str | None = None,
        include_done: bool | None = False,
        operation: CrudOperation | None = None,
    ) -> Cruds:
        prefix = path.CRUD
        if entity_id:
            prefix = path.crud_prefix(entity_id)
        for key in self.storage.iterate_keys(prefix=prefix):
            if not include_done and self.is_done(Path(key).stem):
                continue
            crud: Crud = self.storage.get(key, model=Crud)
            if operation and crud.operation != operation:
                continue
            yield crud

    def put(self, crud: Crud) -> None:
        self.storage.put(crud.storage_path, crud)

    def mark_done(self, crud: Crud) -> None:
        self.tags.touch(_done(str(crud.uuid)))

    def is_done(self, uid: str) -> bool:
        return self.tags.exists(_done(uid))

    def last_execution(self, uid: str) -> datetime | None:
        return self.tags.get(_done(uid))
