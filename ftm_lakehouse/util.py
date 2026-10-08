import multiprocessing
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Callable, Iterable, Iterator, TypeVar, cast

from banal import ensure_list
from followthemoney.dataset.util import dataset_name_check

T = TypeVar("T")

RESERVED_DATASET_NAMES = frozenset({"catalog", "default"})

SAFE_NAME_MAX_LEN = 255
_SAFE_NAME_FORBIDDEN = re.compile(r"[\x00-\x1f\x7f/\\]")
_CHECKSUM_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_QUOTES = re.compile(r"['\"]")


def single_string(value: Any) -> str | None:
    """A single string from a scalar-or-sequence value of length 1, else
    ``None``."""
    value = ensure_list(value)
    if len(value) == 1:
        return str(value[0])


def safe_name(value: str, field: str = "name") -> str:
    """Validate ``value`` as a single path component – for every caller-supplied
    path, storage key or partition value.

    Rejects empty strings, ``.``, anything containing ``..``, ``/``, ``\\`` or
    a control character, and anything longer than `SAFE_NAME_MAX_LEN`.
    """
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, got {type(value).__name__}")
    if not value:
        raise ValueError(f"{field} must not be empty")
    if len(value) > SAFE_NAME_MAX_LEN:
        raise ValueError(f"{field} too long ({len(value)} > {SAFE_NAME_MAX_LEN} chars)")
    if value in (".", ".."):
        raise ValueError(f"{field} `{value}` is a reserved path component")
    if ".." in value:
        raise ValueError(f"{field} `{value}` contains path traversal sequence")
    if _SAFE_NAME_FORBIDDEN.search(value):
        raise ValueError(
            f"{field} `{value!r}` contains forbidden characters "
            "(path separator or control char)"
        )
    return value


@lru_cache(100_000)
def validate_origin(origin: str) -> str:
    """Validate a caller-supplied ``origin`` – it becomes archive paths,
    partition values and SQL string literals.

    `safe_name`, plus no ``'`` / ``"``: a quote would close the literal
    `ParquetStore.delete_origin` builds.
    """
    origin = safe_name(origin, "origin")
    if _QUOTES.search(origin):
        raise ValueError(f"origin `{origin!r}` contains a quote character")
    return origin


def validate_checksum(ch: str) -> str:
    """Validate ``ch`` as a SHA256 digest – exactly 64 lowercase hex chars, so
    it is safe in archive paths."""
    if not isinstance(ch, str) or not _CHECKSUM_RE.fullmatch(ch):
        raise ValueError(
            f"Invalid checksum: `{ch!r}` "
            "(must be 64-character lowercase hex SHA256 digest)"
        )
    return ch


def make_checksum_key(ch: str) -> str:
    """The prefixed path key for a (validated) SHA256 checksum.

    Examples:
        >>> make_checksum_key("a7fdc3...")
        "a7/fd/c3/a7fdc3..."
    """
    validate_checksum(ch)
    return "/".join((ch[:2], ch[2:4], ch[4:6], ch))


_BYTE_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([a-z]*)", re.IGNORECASE)

_BYTE_UNITS = {
    "": 1,
    "b": 1,
    "k": 10**3,
    "kb": 10**3,
    "kib": 2**10,
    "m": 10**6,
    "mb": 10**6,
    "mib": 2**20,
    "g": 10**9,
    "gb": 10**9,
    "gib": 2**30,
    "t": 10**12,
    "tb": 10**12,
    "tib": 2**40,
    "p": 10**15,
    "pb": 10**15,
    "pib": 2**50,
}


def parse_byte_size(value: str) -> int:
    """Parse a DuckDB ``memory_limit``-style size (``64GB``, ``512 MiB``) to
    bytes – decimal or binary units, case insensitive, a bare number is bytes;
    percentages (``80%``) raise ``ValueError``."""
    match = _BYTE_SIZE_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Invalid byte size: `{value}`")
    number, unit = match.groups()
    factor = _BYTE_UNITS.get(unit.lower())
    if factor is None:
        raise ValueError(f"Invalid byte size unit: `{value}`")
    return int(float(number) * factor)


def validate_dataset_name(name: str) -> str:
    """Validate a dataset name at every external entry point: FtM's naming
    rules (lowercase alphanumeric / underscore), not ``catalog`` / ``default``."""
    if not name:
        raise ValueError("Dataset name must not be empty")
    if name in RESERVED_DATASET_NAMES:
        raise ValueError(f"Invalid dataset name: `{name}` (reserved)")
    dataset_name_check(name)
    return name


@contextmanager
def process_map(workers: int) -> Iterator[Callable[..., Iterator[Any]]]:
    """A ``map`` over ``workers`` spawned processes yielding results as they
    finish – the builtin for one.

    Spawned, not forked: the parent holds DuckDB and Delta threads. Pending
    tasks are cancelled when the caller fails.
    """
    if workers <= 1:
        yield map
        return
    context = multiprocessing.get_context("spawn")
    pool = ProcessPoolExecutor(workers, mp_context=context)

    def unordered(fn: Callable[[Any], Any], items: Iterable[Any]) -> Iterator[Any]:
        for future in as_completed([pool.submit(fn, item) for item in items]):
            yield future.result()

    try:
        yield unordered
    finally:
        pool.shutdown(cancel_futures=True)


_DONE = object()


@contextmanager
def prefetch(items: Iterable[T]) -> Iterator[Iterator[T]]:
    """``items`` read one ahead on a thread – for producers that release the
    GIL, like a DuckDB Arrow reader. Leaving the context waits for the read in
    flight, so the producer can be closed right after."""
    source = iter(items)
    with ThreadPoolExecutor(1) as pool:

        def ahead() -> Iterator[T]:
            future = pool.submit(next, source, _DONE)
            while (item := future.result()) is not _DONE:
                future = pool.submit(next, source, _DONE)
                yield cast(T, item)

        yield ahead()
