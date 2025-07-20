import csv
import sys

from followthemoney import StatementEntity
from followthemoney.statement import Statement
from followthemoney.statement.serialize import read_csv_statements
from ftmq.io import smart_read_proxies, smart_write_proxies
from ftmq.types import StatementEntities, Statements, ValueEntities
from ftmq.util import make_dataset

from ftm_lakehouse.conventions import path
from ftm_lakehouse.decorators import skip_if_latest
from ftm_lakehouse.lake.mixins import LakeMixin

csv.field_size_limit(sys.maxsize)


def aggregate_statements(stmts: Statements, dataset: str) -> StatementEntities:
    """This assumes that incoming statements are sorted"""
    ds = make_dataset(dataset)
    statements: list[Statement] = []
    for s in stmts:
        if len(statements) and statements[0].canonical_id != s.canonical_id:
            yield StatementEntity.from_statements(ds, statements)
            statements = []
        statements.append(s)
    if len(statements):
        yield StatementEntity.from_statements(ds, statements)


class DatasetEntities(LakeMixin):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.log.info(
            "👋 DatasetEntities", store=self.storage.get_key(path.ENTITIES_JSON)
        )

    def iterate(self) -> ValueEntities:
        with self.storage.open(path.ENTITIES_JSON) as h:
            yield from smart_read_proxies(h)

    @skip_if_latest(path.ENTITIES_JSON, [path.EXPORTS_STATEMENTS])
    def export(self) -> None:
        with self.storage.open(path.EXPORTS_STATEMENTS) as i:
            statements = read_csv_statements(i)
            entities = aggregate_statements(statements, self.name)
            with self.storage.open(path.ENTITIES_JSON, "wb") as o:
                smart_write_proxies(o, entities)
