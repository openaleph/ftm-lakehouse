"""(De-)Compression logic for export artifacts (entities.ftm.json, statements.csv).

Both directions wrap an *already open* binary handle and hand back another
binary stream, so a codec splices into an existing read / write pipeline
without ever materializing the payload – exports stream row batches into
multi-GB artifacts:

```python
with store.open(key, "wb") as fh, compress(fh, CompressKind.zst) as out:
    smart_write_json(out, entities)
```

The returned stream owns the codec only, never the wrapped handle: closing
it flushes the codec's trailer and leaves ``fh`` open for its own context to
close. **Closing is mandatory** – an unclosed compressor leaves a truncated
frame behind (gzip loses its CRC / size trailer), so always bind the result
in a ``with`` block rather than passing it around bare.

Omitting the algorithm hands ``fh`` straight back, so an uncompressed export
runs through the same call without the caller branching on it. It is
deliberately the handle itself and not a proxy: consumers type-check what
they are handed (anystore's writer treats a non-``IOBase`` as a *uri*), so
only a real file object survives the trip. The one consequence is that
closing the result then closes the handle – which the caller's own ``with``
block does anyway.

Reading mirrors this and needs no seekable source, so a decompressing stream
can sit directly on an HTTP response body.

The wrapped handle is always **binary** – a codec frame is bytes – while
``mode`` describes the stream handed back: ``"rb"`` / ``"wb"`` (default) give
a binary one, ``"r"`` / ``"w"`` a text one for consumers like
:class:`csv.DictReader`. Text is a :class:`io.TextIOWrapper` layered on the
codec (see :func:`_as_mode`), never a codec opened in text mode – both reject
that outright.

``compression.zstd`` only exists on Python 3.14+, so older interpreters pull
:mod:`backports.zstd` – the backport of the very same module, a dependency
guarded by a ``python_version<'3.14'`` marker – and get byte-identical
frames from an identical API.
"""

import sys
from enum import StrEnum
from gzip import GzipFile
from io import TextIOWrapper
from typing import IO, Any, cast

from anystore.logic.constants import DEFAULT_MODE, DEFAULT_WRITE_MODE

if sys.version_info >= (3, 14):
    from compression.zstd import ZstdFile
else:
    from backports.zstd import ZstdFile


class CompressKind(StrEnum):
    """Compress algorithm for export files"""

    gz = "gz"
    zst = "zst"


def _as_mode(stream: IO[bytes], mode: str) -> IO[Any]:
    """Text-wrap ``stream`` unless the caller asked for a binary one.

    Both codecs are binary-only – ``GzipFile`` and ``ZstdFile`` reject
    ``"rt"`` / ``"wt"`` with ``ValueError`` – so a text stream is always a
    :class:`io.TextIOWrapper` on the *outside*, exactly what ``gzip.open()``
    does internally. utf-8 is pinned rather than inheriting the locale, and
    ``newline=""`` leaves line endings untranslated so newlines inside
    quoted csv fields survive the round trip.
    """
    if "b" in mode:
        return stream
    return TextIOWrapper(stream, encoding="utf-8", newline="")


def compress_stream(
    fh: IO[bytes],
    algorithm: CompressKind | None = None,
    mode: str = DEFAULT_WRITE_MODE,
) -> IO[Any]:
    """Stream handler compressing incoming bytes.

    Args:
        fh: Open writable **binary** handle the compressed frame is written
            to – a codec frame is bytes, so this can never be a text handle.
            It is left open when the returned stream closes.
        algorithm: Codec to compress with. Pass ``None`` to omit compression.
        mode: Whether the *returned* stream accepts ``str`` (``"w"``) or
            ``bytes`` (``"wb"``, the default). Only the ``"b"`` is read –
            the direction is the function you called.

    Returns:
        A writable stream. It must be closed – use it as a context manager –
        so the codec flushes its trailer and any text buffer drains; an
        unclosed compressor produces a truncated frame. Without an
        ``algorithm`` *and* in binary mode this is ``fh`` itself, so closing
        it closes the handle.
    """
    if algorithm is None:
        return _as_mode(fh, mode)
    # models both codecs as BufferedIOBase, not IO[bytes], although
    # they implement its full surface (read / write / fileno / iteration).
    if algorithm == CompressKind.zst:
        return _as_mode(cast(IO[bytes], ZstdFile(fh, "wb")), mode)
    # mtime=0 keeps the output byte-identical across runs for identical
    # payloads; the gzip header would otherwise embed the current time.
    return _as_mode(cast(IO[bytes], GzipFile(fileobj=fh, mode="wb", mtime=0)), mode)


def decompress_stream(
    fh: IO[bytes],
    algorithm: CompressKind | None = None,
    mode: str = DEFAULT_MODE,
) -> IO[Any]:
    """Stream handler decompressing incoming bytes.

    Args:
        fh: Open readable **binary** handle carrying the compressed frame.
            Reading is forward-only, so a non-seekable source (an HTTP
            response body) works. The handle is left open when the returned
            stream closes.
        algorithm: Codec the frame was compressed with. Pass ``None`` to omit
            compression.
        mode: Whether the *returned* stream yields ``str`` (``"r"`` – what
            :class:`csv.DictReader` needs) or ``bytes`` (``"rb"``, the
            default). Only the ``"b"`` is read.

    Returns:
        A readable stream yielding the decompressed payload. Without an
        ``algorithm`` *and* in binary mode this is ``fh`` itself.
    """
    if algorithm is None:
        return _as_mode(fh, mode)
    if algorithm == CompressKind.zst:
        return _as_mode(cast(IO[bytes], ZstdFile(fh, "rb")), mode)
    return _as_mode(cast(IO[bytes], GzipFile(fileobj=fh, mode="rb")), mode)
