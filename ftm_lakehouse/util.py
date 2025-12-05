from functools import cache
from typing import Any, TypeVar

from anystore.model import BaseModel
from anystore.types import Uri
from anystore.util import dump_json_model, dump_yaml_model, get_extension
from followthemoney import model
from ftmq.util import SDict
from jinja2 import Template
from rigour.mime import normalize_mimetype, types

T = TypeVar("T", bound=BaseModel)


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


MIME_SCHEMAS = {
    (types.PDF, types.DOCX, types.WORD): model.get("Pages"),
    (types.HTML, types.XML): model.get("HyperText"),
    (types.CSV, types.EXCEL, types.XLS, types.XLSX): model.get("Table"),
    (types.PNG, types.GIF, types.JPEG, types.TIFF, types.DJVU, types.PSD): model.get(
        "Image"
    ),
    (types.OUTLOOK, types.OPF, types.RFC822): model.get("Email"),
    (types.PLAIN, types.RTF): model.get("PlainText"),
}


@cache
def mime_to_schema(mimetype: str) -> str:
    """
    Map a mimetype to a
    [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/)
    File schema.

    Examples:
        >>> mime_to_schema("application/pdf")
        "Pages"

    Args:
        mimetype: The mimetype (will be normalized using `rigour.mime`)

    Returns:
        The schema name as string
    """
    mimetype = normalize_mimetype(mimetype)
    for mtypes, schema in MIME_SCHEMAS.items():
        if mimetype in mtypes:
            if schema is not None:
                return schema.name
    return "Document"


def dump_model(key: Uri, obj: BaseModel) -> bytes:
    """Dump a pydantic model to bytes, either json or yaml (inferred from key
    extension)"""
    ext = get_extension(key)
    if ext == "yml":
        data = dump_yaml_model(obj, clean=True, newline=True)
    elif ext == "json":
        data = dump_json_model(obj, clean=True, newline=True)
    else:
        raise ValueError(f"Invalid extension: `{ext}`")
    return data


def load_model(key: Uri, data: bytes, model: type[T]) -> T:
    """Load a bytes string as a pydantic model, either json or yaml
    (inferred from key extension)"""
    ext = get_extension(key)
    if ext == "yml":
        return model.from_yaml_str(data.decode())
    elif ext == "json":
        return model.from_json_str(data.decode())
    raise ValueError(f"Invalid extension: `{ext}`")


def check_dataset(name: str, data: SDict) -> str:
    if name in ("catalog", "default"):
        raise RuntimeError(f"Invalid dataset name: `{name}`")
    if "dataset" in data and data["dataset"] != name:
        raise RuntimeError(
            "Invalid dataset name: ",
            f"`{data['name']}` (should be: `{name}`)",
        )
    return name
