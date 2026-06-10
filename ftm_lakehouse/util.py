import re
from typing import Any, BinaryIO

from anystore.util import make_checksum as _make_checksum
from anystore.util import make_data_checksum as _make_data_checksum
from followthemoney.dataset.util import dataset_name_check
from jinja2 import Template

from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM

RESERVED_DATASET_NAMES = frozenset({"catalog", "default"})

SAFE_NAME_MAX_LEN = 255
_SAFE_NAME_FORBIDDEN = re.compile(r"[\x00-\x1f\x7f/\\]")
_CHECKSUM_RE = re.compile(r"\A[0-9a-f]{64}\Z")


def make_checksum(io: BinaryIO) -> str:
    """Compute checksum using SHA256."""
    return _make_checksum(io, algorithm=CHECKSUM_ALGORITHM)


def make_data_checksum(data: Any) -> str:
    """Compute data checksum using SHA256."""
    return _make_data_checksum(data, algorithm=CHECKSUM_ALGORITHM)


def safe_name(value: str, field: str = "name") -> str:
    """Validate that ``value`` is safe to use as a single path component.

    Rejects empty strings, path traversal sequences (``..``), the current-
    directory marker (``.``), path separators (``/`` and ``\\``), control
    characters (including null and DEL), and anything longer than
    :data:`SAFE_NAME_MAX_LEN`.

    Use this for every caller-supplied string that flows into a filesystem
    path, storage key, or partition value. ``origin``, ``file_id``, bucket
    sub-paths, etc. should all go through here.

    Args:
        value: The candidate string.
        field: Human-readable field name, used to make error messages
            informative.

    Returns:
        ``value`` unchanged if valid.

    Raises:
        ValueError: If ``value`` is empty, too long, equal to ``.`` or
            ``..``, contains ``..`` as a substring, contains a path
            separator, or contains a control character.
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


def validate_origin(origin: str) -> str:
    """Validate an ``origin`` tag for safe use as a path / partition value.

    ``origin`` is caller-supplied (journal rows, crawl callers, API
    requests) and flows directly into archive file paths
    (:func:`ftm_lakehouse.core.conventions.path.archive_txt`) and parquet
    partition prefixes
    (:func:`ftm_lakehouse.core.conventions.path.statement_origin`).
    Without validation a traversal sequence in ``origin`` escapes the
    archive subtree or writes a parquet partition outside
    ``entities/statements/``.

    Built on :func:`safe_name`; allows everything ``safe_name`` allows
    (printable, no separators, no traversal), which covers conventional
    origins like ``default``, ``crawl``, ``mapping:abc123…``, ``source-a``.

    Args:
        origin: The candidate origin tag.

    Returns:
        ``origin`` unchanged if valid.

    Raises:
        ValueError: As per :func:`safe_name`.
    """
    return safe_name(origin, "origin")


def validate_checksum(ch: str) -> str:
    """Validate that ``ch`` is a valid SHA256 hex digest.

    Enforces exactly 64 lowercase hex characters (``[0-9a-f]``) so that
    ``ch`` is safe to interpolate into archive paths without traversal
    risk.

    Args:
        ch: The candidate checksum string.

    Returns:
        ``ch`` unchanged if valid.

    Raises:
        ValueError: If ``ch`` is not a 64-character lowercase hex string.
    """
    if not isinstance(ch, str) or not _CHECKSUM_RE.fullmatch(ch):
        raise ValueError(
            f"Invalid checksum: `{ch!r}` "
            "(must be 64-character lowercase hex SHA256 digest)"
        )
    return ch


def make_checksum_key(ch: str) -> str:
    """
    Generate a path key for the given SHA256 checksum.

    Examples:
        >>> make_checksum_key("a7fdc3...")
        "a7/fd/c3/a7fdc3..."

    Args:
        ch: SHA256 Hex checksum (content_hash)

    Raises:
        ValueError: If the checksum is not a valid SHA256 hex digest

    Returns:
        The prefixed path
    """
    validate_checksum(ch)
    return "/".join((ch[:2], ch[2:4], ch[4:6], ch))


def render(tmpl: str, data: dict[str, Any]) -> str:
    """
    Shorthand for jinja2 template rendering

    Examples:
        >>> render("hello: {{ hello }}", {"hello": "world"})
        "hello: world"
    """
    template = Template(tmpl)
    return template.render(**data)


def validate_dataset_name(name: str) -> str:
    """Validate a dataset name against FollowTheMoney's naming rules and
    the lakehouse's reserved-name list.

    The same check is used at every external entry point (API, CLI,
    :class:`Catalog`) so that a dataset name can be trusted as it flows
    into path construction, SQL identifiers, and DuckDB queries downstream.

    Args:
        name: The candidate dataset name.

    Returns:
        ``name`` if valid.

    Raises:
        ValueError: If ``name`` is empty, fails ``dataset_name_check``
            (lowercase alphanumeric / underscore only), or is reserved
            (``catalog`` / ``default``).
    """
    if not name:
        raise ValueError("Dataset name must not be empty")
    if name in RESERVED_DATASET_NAMES:
        raise ValueError(f"Invalid dataset name: `{name}` (reserved)")
    dataset_name_check(name)
    return name
