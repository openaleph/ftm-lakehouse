"""Dataset metadata model + the process-wide model-class hook."""

from string import Template

from anystore.model import StoreModel
from anystore.types import HttpUrlStr
from ftmq.model import Dataset

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.logic.compress import CompressKind

settings = Settings()

DEFAULT_SHARDS = 0
"""Hardcoded shard-count default – a single shard. The shard count is
per-dataset configuration (``config.yml``), set once at creation; there is
deliberately no environment override so changing environments can't
mis-shard a dataset."""


class DatasetModel(Dataset):
    storage: StoreModel | None = None
    """Set storage for external lakehouse"""
    public_url_prefix: HttpUrlStr | None = None
    """Public url prefix for resources"""
    shards: int = DEFAULT_SHARDS
    """Number of entity-id hash shards for the parquet store. ``0`` (default)
    means a single shard; huge datasets should configure ``8`` or more at
    creation for bounded per-partition working sets (e.g.
    ``ensure_dataset("big_leak", shards=8)``). Fixed once the store is
    written: setting it here only changes where readers *look*, so changing
    it after the fact means a full rewrite –
    :class:`~ftm_lakehouse.operation.maintenance.ShardOperation`."""
    compression: CompressKind | None = None
    """Compress exported artifacts (statements.csv, entities.ftm.json, diffs...)"""

    def get_public_prefix(self) -> str | None:
        if self.public_url_prefix:
            return self.public_url_prefix
        if settings.public_url_prefix:
            # ``${dataset}`` placeholder; safe_substitute so literal ``$``
            # (and ``%``) in a prefix never breaks
            return Template(settings.public_url_prefix).safe_substitute(
                dataset=self.name
            )


_model_class: type[DatasetModel] = DatasetModel


def set_model_class(model_class: type[DatasetModel]) -> None:
    """Register a custom :class:`DatasetModel` subclass process-wide.

    Every config read – repository construction, ``get_dataset_model``,
    ``update_dataset``, the index export – constructs models via
    :func:`get_model_class`, so downstream applications extend the dataset
    config schema with one call at process start:

    ```python
    import ftm_lakehouse

    class MyModel(ftm_lakehouse.DatasetModel):
        my_field: str | None = None

    ftm_lakehouse.set_model_class(MyModel)
    ```

    Call this **before** any repository or config access – repositories
    snapshot their model at construction and are LRU-cached, so a later
    switch requires ``repository.factories.clear_caches()``.

    Args:
        model_class: The :class:`DatasetModel` subclass to use.
    """
    global _model_class
    _model_class = model_class


def get_model_class() -> type[DatasetModel]:
    """The registered :class:`DatasetModel` class (see :func:`set_model_class`)."""
    return _model_class
