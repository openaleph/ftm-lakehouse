from typing import Any

from anystore.model import BaseModel
from anystore.types import Uri
from anystore.util import dump_json_model, dump_yaml_model, get_extension
from followthemoney import model
from jinja2 import Template
from rigour.mime import types


def make_checksum_key(ch: str) -> str:
    if len(ch) != 40:  # sha1
        raise ValueError(f"Invalid checksum: `{ch}`")
    return "/".join((ch[:2], ch[2:4], ch[4:6], ch))


def render(tmpl: str, data: dict[str, Any]) -> str:
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


def mime_to_schema(mimetype: str) -> str:
    for mtypes, schema in MIME_SCHEMAS.items():
        if mimetype in mtypes:
            if schema is not None:
                return schema.name
    return "Document"


def dump_model(key: Uri, obj: BaseModel) -> bytes:
    ext = get_extension(key)
    if ext == "yml":
        data = dump_yaml_model(obj, clean=True, newline=True)
    elif ext == "json":
        data = dump_json_model(obj, clean=True, newline=True)
    else:
        raise ValueError(f"Invalid extension: `{ext}`")
    return data


def load_model(key: Uri, data: bytes, model: type[BaseModel]) -> BaseModel:
    ext = get_extension(key)
    if ext == "yml":
        return model.from_yaml_str(data.decode())
    elif ext == "json":
        return model.from_json_str(data.decode())
    raise ValueError(f"Invalid extension: `{ext}`")
