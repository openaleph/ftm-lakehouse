from ftmq.store.fragments import get_dataset
from ftmq.store.lake import DEFAULT_ORIGIN

from ftm_lakehouse.conventions import tag
from ftm_lakehouse.lake.mixins import LakeMixin
from ftm_lakehouse.lake.statements import DatasetStatements
from ftm_lakehouse.settings import Settings


class DatasetFragments(LakeMixin):
    def __init__(self, statements: DatasetStatements, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.statements = statements
        settings = Settings()
        self.store = get_dataset(self.name, databse_uri=settings.fragments_uri)
        self.get_entity = self.store.get
        self.log.info("👋 DatasetFragments", store=settings.fragments_uri)

    def flush(self, origin: str | None = DEFAULT_ORIGIN) -> None:
        """Load the fragments into the given statement store origin. After it,
        the fragment store is completely erased"""
        with self.tags.touch(tag.FRAGMENTS_COLLECTED):
            with self.statements.bulk(origin) as bulk:
                for fragment in self.store.iterate():
                    # FIXME respect fragment origin
                    bulk.add_entity(fragment)
            self.store.drop()
