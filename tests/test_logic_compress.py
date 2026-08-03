"""Streaming (de-)compression of export artifacts.

The load-bearing properties are *streaming* (no full-payload buffering, no
seekable source required) and *handle ownership*: the codec stream wraps a
handle it must not close, so it composes with ``store.open(...)`` contexts.
"""

import csv
import io

import pytest

from ftm_lakehouse.logic.compress import (
    CompressKind,
    compress_stream,
    decompress_stream,
)

MAGIC = {CompressKind.gz: b"\x1f\x8b", CompressKind.zst: b"\x28\xb5\x2f\xfd"}


class NonSeekable(io.RawIOBase):
    """A forward-only source, like an HTTP response body."""

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_roundtrip(algorithm):
    payload = b'{"id":"jane","schema":"Person"}\n' * 1_000

    raw = io.BytesIO()
    with compress_stream(raw, algorithm) as out:
        out.write(payload)
    data = raw.getvalue()

    assert data.startswith(MAGIC[algorithm])
    assert len(data) < len(payload)  # actually compressed

    with decompress_stream(io.BytesIO(data), algorithm) as fh:
        assert fh.read() == payload


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_compress_streams_chunks(algorithm):
    """Chunked writes and line-wise reads – neither side buffers the whole
    payload, which is what makes multi-GB exports possible."""
    lines = [b"line %d\n" % i for i in range(5_000)]

    raw = io.BytesIO()
    with compress_stream(raw, algorithm) as out:
        for line in lines:
            out.write(line)

    with decompress_stream(io.BytesIO(raw.getvalue()), algorithm) as fh:
        assert list(fh) == lines


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_decompress_non_seekable_source(algorithm):
    raw = io.BytesIO()
    with compress_stream(raw, algorithm) as out:
        out.write(b"hello lakehouse")

    with decompress_stream(NonSeekable(raw.getvalue()), algorithm) as fh:
        assert fh.read() == b"hello lakehouse"


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_wrapped_handle_stays_open(algorithm):
    """Closing the codec must not close the wrapped handle – exports write
    inside a ``store.open()`` context that closes it itself."""
    raw = io.BytesIO()
    with compress_stream(raw, algorithm) as out:
        out.write(b"payload")
    assert not raw.closed
    data = raw.getvalue()
    raw.close()

    source = io.BytesIO(data)
    with decompress_stream(source, algorithm) as fh:
        fh.read()
    assert not source.closed


def test_gzip_output_is_deterministic():
    """mtime=0 – re-exporting an unchanged payload yields identical bytes."""

    def run() -> bytes:
        raw = io.BytesIO()
        with compress_stream(raw, CompressKind.gz) as out:
            out.write(b"payload")
        return raw.getvalue()

    assert run() == run()


def test_no_algorithm_passes_the_handle_through():
    """``None`` hands back the handle itself, and it must stay a real file
    object: consumers type-check what they get – anystore's writer treats a
    non-``IOBase`` as a *uri*, so a transparent proxy here silently wrote
    empty exports."""
    raw = io.BytesIO()
    out = compress_stream(raw, None)
    assert out is raw
    assert isinstance(out, io.IOBase)
    out.write(b"payload")
    assert raw.getvalue() == b"payload"

    source = io.BytesIO(b"payload")
    assert decompress_stream(source, None) is source


def test_default_algorithm_is_none():
    raw = io.BytesIO()
    out = compress_stream(raw)
    out.write(b"payload")
    assert raw.getvalue() == b"payload"


class Sink(io.BytesIO):
    """A ``BytesIO`` with a no-op ``close()``.

    The wrapped handle is binary in every combination; this just keeps
    ``getvalue()`` reachable after the codec (or its text wrapper) closes on
    the way out of the ``with`` block.
    """

    def close(self) -> None:
        pass


@pytest.mark.parametrize("algorithm", [None, *CompressKind])
@pytest.mark.parametrize("mode", ["r", "rb"])
def test_stream_text_and_binary_modes(mode, algorithm):
    """``mode`` describes the returned stream, not the wrapped handle – which
    stays binary, because a codec frame is bytes either way."""
    text = "päyload,with,commas\nsecond line\n"
    payload = text if "b" not in mode else text.encode()

    sink = Sink()
    with compress_stream(sink, algorithm, mode=mode.replace("r", "w")) as out:
        out.write(payload)
    data = sink.getvalue()

    assert isinstance(data, bytes)  # the handle never sees str
    if algorithm is not None:
        assert data.startswith(MAGIC[algorithm])

    with decompress_stream(io.BytesIO(data), algorithm, mode=mode) as fh:
        assert fh.read() == payload


def test_text_mode_pins_utf8():
    """Not the locale's preferred encoding – artifacts must not depend on the
    environment that wrote them."""
    sink = Sink()
    with compress_stream(sink, None, mode="w") as out:
        out.write("päyload")
    assert sink.getvalue() == "päyload".encode("utf-8")


def test_text_mode_feeds_csv_reader():
    """The motivating case: statements.csv read back through the codec."""
    sink = Sink()
    with compress_stream(sink, CompressKind.gz, mode="w") as out:
        out.write('id,value\r\n1,"a,b"\r\n')

    with decompress_stream(io.BytesIO(sink.getvalue()), CompressKind.gz, "r") as fh:
        rows = list(csv.DictReader(fh))
    assert rows == [{"id": "1", "value": "a,b"}]
