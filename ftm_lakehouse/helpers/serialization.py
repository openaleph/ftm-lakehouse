from anystore.model import BaseModel
from anystore.types import M, Uri
from anystore.util import dump_json_model, dump_yaml_model, get_extension

YAML = ("yml", "yaml")


def dump_model(key: Uri, obj: BaseModel) -> bytes:
    """Dump a pydantic model to bytes, either json (the default) or yaml
    (inferred from key extension)"""
    ext = get_extension(key)
    if ext in YAML:
        return dump_yaml_model(obj, clean=True, newline=True)
    return dump_json_model(obj, clean=True, newline=True)


def load_model(key: Uri, data: bytes, model: type[M]) -> M:
    """Load a bytes string as a pydantic model, either json (the default) or
    yaml (inferred from key extension)"""
    ext = get_extension(key)
    if ext in YAML:
        return model.from_yaml_str(data.decode())
    return model.from_json_str(data.decode())
