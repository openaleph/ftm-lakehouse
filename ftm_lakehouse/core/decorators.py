from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable

from anystore.decorators import anycache
from anystore.model import BaseModel
from anystore.util import ensure_uuid

if TYPE_CHECKING:
    from ftm_lakehouse.core.mixins import LakeMixin, StorageMixin


def versioned(path: str) -> Callable[..., Any]:
    """
    Write the returning pydantic object of the wrapped function to `path` (yml
    or json) and store a time-based version of it in a snapshot directory
    (versions/YYYY/MM/timestamp/filename). The wrapped function must return a
    pydantic object.

    Classes using this decorator need to subclass from
    [StorageMixin][ftm_lakehouse.mixins.StorageMixin]
    """

    def _decorator(func: Callable[..., BaseModel]):
        def _inner(self: "StorageMixin", *args, **kwargs):
            obj = func(self, *args, **kwargs)
            self.versions.make(path, obj)
            return obj

        return _inner

    return _decorator


def skip_if_latest(key: str, dependencies: Iterable[str]) -> Callable[..., Any]:
    """
    Skip the wrapped method if all of the last runs of the given dependencies
    are prior than the last run of the wrapped function. The wrapped function
    must return `None`.

    Methods using this decorator need to subclass from
    [LakeMixin][ftm_lakehouse.mixins.LakeMixin]

    Args:
        key: The tag key to store timestamp
        dependencies: Other timestamp tag keys that need to be lower for
            skipping
    """

    def _decorator(func: Callable[..., BaseModel | None]):
        def _inner(self: "LakeMixin", *args, **kwargs):
            if self.tags.is_latest(key, dependencies):
                self.log.info(
                    f"Already up-to-date: `{key}`, skipping ...",
                    key=key,
                    dependencies=dependencies,
                )
                return

            with self.tags.touch(key) as now:
                run_id = ensure_uuid()
                self.log.info(
                    f"Start `{key}` ...",
                    key=key,
                    dependencies=dependencies,
                    started=now,
                    run_id=run_id,
                )
                _ = func(self, *args, **kwargs)
                self.log.info(
                    f"Done `{key}`.",
                    key=key,
                    dependencies=dependencies,
                    started=now,
                    run_id=run_id,
                    took=(datetime.now() - now).seconds,
                )

        return _inner

    return _decorator


def storage_cache(key_func: Callable[..., str | None], **kwargs):
    """
    extend @anystore.decorators.anycache decorator to inject proper cache
    storage and key prefix
    """

    def _decorator(func: Callable[..., Any]):
        def _inner(self: "LakeMixin", *fargs, **fkwargs):
            return anycache(
                store=self.cache,
                key_func=lambda *a, **kw: self.make_cache_key(key_func(*a, **kw)),
                **kwargs,
            )(func)(self, *fargs, **fkwargs)

        return _inner

    return _decorator
