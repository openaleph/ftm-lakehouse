from typing import Any, BinaryIO

from anystore.types import SDict
from anystore.util import make_checksum as _make_checksum
from anystore.util import make_data_checksum as _make_data_checksum
from followthemoney import StatementEntity
from ftmq.util import make_entity as _make_entity
from jinja2 import Template

from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM


def make_checksum(io: BinaryIO) -> str:
    """Compute checksum using SHA256."""
    return _make_checksum(io, algorithm=CHECKSUM_ALGORITHM)


def make_data_checksum(data: Any) -> str:
    """Compute data checksum using SHA256."""
    return _make_data_checksum(data, algorithm=CHECKSUM_ALGORITHM)


def validate_checksum(ch: str) -> str:
    """Validate that a checksum is a valid SHA256 hex digest (64 chars).

    Raises:
        ValueError: If the checksum is not a valid SHA256 hex digest
    """
    if len(ch) != 64:
        raise ValueError(f"Invalid checksum: `{ch}`")
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


def check_dataset(name: str, data: SDict) -> str:
    if name in ("catalog", "default"):
        raise RuntimeError(f"Invalid dataset name: `{name}`")
    if "dataset" in data and data["dataset"] != name:
        raise RuntimeError(
            "Invalid dataset name: ",
            f"`{data['name']}` (should be: `{name}`)",
        )
    return name


def make_entity(data: SDict, dataset: str) -> StatementEntity:
    return _make_entity(data, StatementEntity, dataset)


def make_envelope(data: SDict, op: str = "ADD") -> SDict:
    """Create diff action envelope for data dict

    Ref. https://www.opensanctions.org/docs/bulk/delta/
    """
    return {"op": op, "entity": data}
