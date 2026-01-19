from typing import Any

from anystore.types import SDict
from followthemoney import StatementEntity
from ftmq.util import make_entity as _make_entity
from jinja2 import Template


def make_checksum_key(ch: str) -> str:
    """
    Generate a path key for the given SHA1 checksum

    Examples:
        >>> make_checksum_key("5a6acf229ba576d9a40b09292595658bbb74ef56")
        "5a/6a/cf/5a6acf229ba576d9a40b09292595658bbb74ef56"

    Args:
        ch: SHA1 checksum (often referred to as `content_hash`)

    Raises:
        ValueError: If the checksum is not 40 chars long (SHA1)

    Returns:
        The prefixed SHA1 path
    """
    if len(ch) != 40:  # sha1
        raise ValueError(f"Invalid checksum: `{ch}`")
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
