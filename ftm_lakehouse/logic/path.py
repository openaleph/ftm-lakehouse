"""Path primitives for the storage-layout conventions.

The convention layer builds its named paths on top
([`ftm_lakehouse.core.conventions.path`][ftm_lakehouse.core.conventions.path]).

Store **keys**, not filesystem paths, hence `PurePosixPath`: always
``/``-joined whatever the host OS, without the filesystem I/O a key has no
business exposing, and still `os.PathLike` so anystore takes one as a ``Uri``.

Two transforms compose the layout's variants, as operators:

    ENTITIES_JSON + compression           # entities.ftm.json.zst
    EXPORTS_DOCUMENTS[origin]             # exports/documents.crawl.csv
    EXPORTS_DOCUMENTS[origin] + codec     # exports/documents.crawl.csv.zst

Both take ``None`` unchanged, so an optional codec or scope passes straight
through without a branch at the call site.
"""

from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

from anystore.util import ensure_uuid
from normality import stringify
from rigour.time import utc_now

from ftm_lakehouse.util import safe_name


def make_ts(ts: datetime | None = None, format: str | None = None) -> str:
    """Render a timestamp in the format, defaulting to now."""
    ts = ts or utc_now()
    if format:
        return ts.strftime(format)
    return ts.isoformat()


class StoreKey(PurePosixPath):
    def with_segments(self, *segments: Any) -> "StoreKey":
        """Build a derived key, dropping any builder identity."""
        return StoreKey(*segments)

    def __eq__(self, other: object) -> bool:
        """A key equals its own spelling, so a string can address it.

        `PurePosixPath` compares only to other paths while hashing as its
        string – which puts a string lookup into the right bucket and then
        rejects it, a *silent* miss. Closing that leaves no silent failure
        mode: the string APIs a key still cannot reach (``key in text``,
        ``text.startswith(key)``, ``"a" + key``) all raise instead.

        The comparison is against the normalised spelling, so
        ``StoreKey("a//b") == "a/b"`` – which is the right answer for a key.
        """
        if isinstance(other, str):
            return str(self) == other
        return super().__eq__(other)

    # defining `__eq__` would otherwise drop the inherited hash, and it is
    # already the hash of the string – which is what makes the two
    # interchangeable as dict keys
    __hash__ = PurePosixPath.__hash__

    def __getitem__(self, infix: Any | None = None) -> "StoreKey":
        """``DOCUMENTS["crawl"]`` -> ``documents.crawl.csv``"""
        infix_ = stringify(infix)
        if infix_ is None:
            return StoreKey(self)
        stem, dot, extension = self.name.partition(".")
        return self.with_name(f"{stem}.{safe_name(infix_, 'infix')}{dot}{extension}")

    def __iter__(self) -> Any:
        """A key is one path, not a sequence of them.

        Without this, `__getitem__` makes the legacy iteration protocol apply –
        and since subscripting never raises `IndexError`, ``"a" in key`` would
        spin forever building infixes.
        """
        raise TypeError(f"{type(self).__name__} is not iterable")

    def __add__(self, suffix: Any | None = None) -> "StoreKey":
        """``STATEMENTS + "zst"`` -> ``statements.csv.zst``"""
        suffix_ = stringify(suffix)
        if suffix_ is None:
            return StoreKey(self)
        return self.with_name(f"{self.name}.{safe_name(suffix_.strip('.'), 'suffix')}")


class ScopedKey(StoreKey):
    """A key whose last segment is a swappable scope, carrying a default.

    Renders with the default already applied, so the common case needs no
    subscript at all – and ``[]`` selects another scope, the same "variant of
    this" reading `StoreKey.__getitem__` has, one path segment up::

        TAGS / "foo"              # tags/lakehouse/foo
        TAGS["tenant1"] / "foo"   # tags/tenant1/foo

    Swapping yields a plain `StoreKey`, so a scope is picked once rather than
    re-picked down a chain.
    """

    def __getitem__(self, scope: str | None = None) -> "StoreKey":
        """``TAGS["tenant1"]`` -> ``tags/tenant1``"""
        if scope is None:
            return StoreKey(self)
        return self.with_name(safe_name(scope, "scope"))


class CallableKey(StoreKey):
    """A key that is also a factory for the keys underneath it.

    For a convention whose base path is addressable on its own *and*
    parameterised, e.g.: ``versions/`` is a real prefix to iterate, and
    ``versions(name, ts)`` is one snapshot in it. Subclasses implement
    [`__call__`][CallableKey.__call__].
    """

    def __call__(self, *args: Any, **kwargs: Any) -> StoreKey:
        raise NotImplementedError


class DateTimeKey(CallableKey):
    """A directory whose entries are named after a point in time.

    The directory is addressable on its own – a series to list, and the thing
    a freshness tag can be named after – while calling it names one entry in
    it::

        SERIES                    # diffs/entities.ftm.json
        SERIES(ts)                # diffs/entities.ftm.json/{ts}.delta.json
        SERIES(ts) + codec        # .../{ts}.delta.json.zst

    `ext` describes what the series *holds*, so it lands on entries and
    never on the directory – which stays extension- and codec-free however
    the files are named. That matters wherever the directory doubles as a tag
    or state key: neither may move when a dataset changes its compression.

    ``[]`` varies the directory's name (`StoreKey.__getitem__`) and stays a
    `DateTimeKey`, since a variant has entries of its own.

    Args:
        base: The directory the entries live in.
        format: `datetime.strftime` format for an entry's name. ``None``
            renders ISO 8601.
        ext: Extension of one entry. ``None`` leaves entries bare.
    """

    def __init__(
        self, base: StoreKey | str, format: str | None = None, ext: str | None = None
    ) -> None:
        super().__init__(base)
        self.format = format
        self.ext = ext

    def __getitem__(self, infix: str | None = None) -> "DateTimeKey":
        """``SERIES["crawl"]`` -> the ``crawl`` variant, still a series

        The extension rides along to the variant's entries rather than being
        applied here: it belongs to what the series *holds*, and the directory
        itself has to stay clean.
        """
        return DateTimeKey(StoreKey(self)[infix], self.format, self.ext)

    def __call__(self, ts: datetime | None = None) -> StoreKey:
        """One entry, named for ``ts`` – the current time when omitted."""
        return self / make_ts(ts, self.format) + self.ext


class JobsKey(CallableKey):
    """``jobs/runs/``: a prefix per job type, and a factory for one run."""

    def __getitem__(self, name: str | None = None) -> StoreKey:
        """``JOB_RUNS["ExportJob"]`` -> ``jobs/runs/ExportJob``"""
        if not name:
            raise ValueError("Missing job `name`")
        return self / safe_name(name, "job_type")

    def __call__(self, name: str, run_id: str | None = None) -> StoreKey:
        """One job run's result.

        Layout: ``jobs/runs/{job_type}/{run_id}.json``

        Args:
            name: Job type – the job model's class name
            run_id: Id of the run, omit to mint one. `ensure_uuid` is
                time-ordered (uuid7), so runs sort chronologically by key and
                the newest is the last one listed.

        Returns:
            Key of the run result
        """
        return self[name] / f"{run_id or ensure_uuid()}.json"
