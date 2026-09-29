"""The data path: ``zfs send`` out as chunks, chunks into ``zfs receive``.

Both sides put an in-memory buffer between the pipe and HTTP – the role
``mbuffer -m`` played – sized by ``LAKEHOUSE_ZFS_BUFFER``.
"""

import os
import queue
import threading
from collections.abc import AsyncIterator, Iterable, Iterator
from contextlib import suppress
from functools import partial
from typing import Any

import anyio
import anyio.to_thread
from zfs_agent import zfs_receive, zfs_send

from ftm_lakehouse.core.zfs.util import (
    CHUNK_SIZE,
    END,
    POLL,
    Worker,
    pipe,
    put,
    read_chunk,
    slots,
    write_all,
)


class SendStream:
    """``zfs send`` of a dataset as an iterable of chunks.

    A thread drains the pipe into an in-memory read-ahead of up to
    ``buffer`` bytes, so a stalling network doesn't stall the send and vice
    versa. `close` stops it from any thread at any point – the reader closes
    the pipe and the send fails on EPIPE instead of blocking. That has to be
    explicit: a consumer that just stops iterating (an HTTP client gone
    mid-download) leaves the iterator unclosed until garbage collection.

    Iterating – sync, or async without holding a worker thread – yields
    chunks of up to ``CHUNK_SIZE`` bytes, then raises ``RuntimeError`` if
    ``zfs send`` failed, after the stream it produced.
    """

    def __init__(
        self,
        dataset: str,
        buffer: int,
        snapshot: str | None = None,
        since: str | None = None,
        token: str | None = None,
    ) -> None:
        r, w = pipe()
        self._send = Worker(
            zfs_send, dataset, w, snapshot=snapshot, since=since, token=token
        )
        self._send.start()
        self._chunks: "queue.Queue[Any]" = queue.Queue(maxsize=slots(buffer))
        self._closed = threading.Event()
        self._ended = False
        threading.Thread(target=self._read, args=(r,), daemon=True).start()

    def _alive(self) -> bool:
        return not self._closed.is_set()

    def _read(self, fd: int) -> None:
        try:
            while chunk := read_chunk(fd, self._alive):
                if not put(self._chunks, chunk, self._alive):
                    return
        finally:
            os.close(fd)
            put(self._chunks, END, self._alive)

    def first(self) -> bytes:
        """Wait for the first chunk, so a send that fails before producing
        any raises here – before an HTTP response commits to a status.
        Iterating afterwards continues after it."""
        chunk = self._chunks.get()
        if chunk is END:
            self._ended = True
            self._send.result()
            return b""
        first: bytes = chunk
        return first

    def __iter__(self) -> Iterator[bytes]:
        while not self._ended:
            try:
                chunk = self._chunks.get(timeout=POLL)
            except queue.Empty:
                if self._closed.is_set():
                    return
                continue
            if chunk is END:
                self._ended = True
                self._send.result()
                return
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while not self._ended:
            try:
                chunk = self._chunks.get_nowait()
            except queue.Empty:
                if self._closed.is_set():
                    return
                await anyio.sleep(0.01)
                continue
            if chunk is END:
                self._ended = True
                await anyio.to_thread.run_sync(self._send.result)
                return
            yield chunk

    def close(self) -> None:
        """Stop streaming; the send ends on EPIPE."""
        self._closed.set()


class ReceiveFeed:
    """Feed chunks into ``zfs receive`` through an in-memory buffer.

    A thread writes queued chunks into the pipe the receive reads from.
    `put` queues from sync code, blocking while the buffer is full; `offer`
    only queues if there's room, for async code that waits by sleeping
    rather than holding a worker thread. `close` ends the stream and raises
    the receive's error.
    """

    def __init__(self, dataset: str, buffer: int, **kwargs: Any) -> None:
        r, self._w = pipe()
        self._receive = Worker(zfs_receive, dataset, r, **kwargs)
        self._receive.start()
        self._chunks: "queue.Queue[Any]" = queue.Queue(maxsize=slots(buffer))
        self._writer = threading.Thread(target=self._write, daemon=True)
        self._writer.start()

    def _write(self) -> None:
        try:
            while (chunk := self._chunks.get()) is not END:
                write_all(self._w, chunk)
        except BrokenPipeError:
            pass  # the receive is gone; close() reports why
        finally:
            os.close(self._w)

    @property
    def alive(self) -> bool:
        """Whether the receive still takes chunks."""
        return self._writer.is_alive()

    def put(self, chunk: bytes) -> bool:
        """Queue a chunk; ``False`` once the receive stopped taking any."""
        return put(self._chunks, chunk, lambda: self.alive)

    def offer(self, chunk: bytes) -> bool:
        """Queue a chunk if the buffer has room right now."""
        try:
            self._chunks.put_nowait(chunk)
        except queue.Full:
            return False
        return True

    def close(self) -> None:
        """End the stream and wait for the receive.

        Raises:
            RuntimeError: when ``zfs receive`` failed – including on a
                stream that was cut short.
        """
        put(self._chunks, END, lambda: self.alive)
        self._writer.join()
        self._receive.result()


def rechunk(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Merge small chunks (as HTTP delivers them) into ~``CHUNK_SIZE`` ones."""
    buf = bytearray()
    for chunk in chunks:
        buf += chunk
        if len(buf) >= CHUNK_SIZE:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)


async def arechunk(chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """``rechunk`` for an async stream."""
    buf = bytearray()
    async for chunk in chunks:
        buf += chunk
        if len(buf) >= CHUNK_SIZE:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)


def run_receive(
    dataset: str, chunks: Iterable[bytes], buffer: int, **kwargs: Any
) -> None:
    """Receive a stream of ``chunks`` into ``dataset``.

    Raises:
        RuntimeError: when ``zfs receive`` failed. If feeding it failed
            first – a network error, Ctrl-C – that error is raised instead:
            the receive then only failed because its stream was cut short.
    """
    feed = ReceiveFeed(dataset, buffer, **kwargs)
    try:
        for chunk in rechunk(chunks):
            if not feed.put(chunk):
                break
    except BaseException:
        with suppress(Exception):
            feed.close()
        raise
    feed.close()


async def arun_receive(
    dataset: str, chunks: AsyncIterator[bytes], buffer: int, **kwargs: Any
) -> None:
    """``run_receive`` for an async stream, e.g. an HTTP request body –
    waiting on a full buffer by sleeping, not by holding a worker thread.

    A receive that fails early still has the rest of ``chunks`` read (and
    dropped): a server that stops reading a request body without closing
    the connection – granian does that – leaves an HTTP client blocked
    writing, never to see the error.
    """
    feed = await anyio.to_thread.run_sync(
        partial(ReceiveFeed, dataset, buffer, **kwargs)
    )
    rechunked = arechunk(chunks)
    try:
        async for chunk in rechunked:
            while feed.alive and not feed.offer(chunk):
                await anyio.sleep(0.01)
            if not feed.alive:
                async for _ in rechunked:
                    pass
                break
    except BaseException:
        # the receive must see its stream end even when cancelled
        with anyio.CancelScope(shield=True), suppress(Exception):
            await anyio.to_thread.run_sync(feed.close)
        raise
    with anyio.CancelScope(shield=True):
        await anyio.to_thread.run_sync(feed.close)
