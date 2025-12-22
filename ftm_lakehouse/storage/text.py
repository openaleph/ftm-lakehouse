"""TextStore - extracted text (str) from source files"""

from typing import Literal

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.storage.base import DEFAULT_ORIGIN, StrStorage


class TextStore(StrStorage[Literal[False]]):
    """
    Extracted fulltext storage. Writes and reads "str" typed data.

    Text is stored with an origin identifier that should be the extraction
    engine (+ version) or will be "default".

    Text can be retrieved by the source file checksum and optional origin.

    Path to txt: archive/ab/cd/ef/abcdef1234.../{origin}.txt
    """

    serialization_mode = "auto"
    raise_on_nonexist = False

    def put(
        self, checksum: str, value: str, origin: str | None = DEFAULT_ORIGIN
    ) -> None:
        """Store fulltext for the given source checksum and origin"""
        origin = origin or DEFAULT_ORIGIN
        key = path.archive_txt(checksum, origin)
        self._store.put(key, value)

    def get(self, checksum: str, origin: str | None = None) -> str | None:
        """Retrieve fulltext for the given source checksum and optional origin,
        otherwise an arbitrary text result will be returned or None if nothing
        found."""
        if origin:
            key = path.archive_txt(checksum, origin)
            return self._store.get(key)
        for value in self._store.iterate_values(
            prefix=path.archive_prefix(checksum), glob="*.txt"
        ):
            return value
