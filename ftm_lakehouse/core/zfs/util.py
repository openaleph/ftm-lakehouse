"""Pipe and queue plumbing between a blocking ``zfs`` call and HTTP.

A ``zfs send`` / ``receive`` runs (via ``zfs-agent``) on one end of an
``os.pipe()``; the other end is drained into, or filled from, a queue of
``CHUNK_SIZE`` chunks. The helpers here know nothing about ZFS.
"""

import os
import queue
import select
import sys
import threading
from collections.abc import Callable
from contextlib import suppress
from typing import Any

if sys.platform != "win32":
    import fcntl

CHUNK_SIZE = 4 * 1024 * 1024
"""Bytes per chunk between a pipe and HTTP: few enough Python round trips
per second at network speed."""

PIPE_SIZE = 1024 * 1024  # Linux's default ceiling for an unprivileged pipe
POLL = 0.1  # seconds a blocked stream waits before checking it was stopped
END = object()  # queued after the last chunk


class Worker(threading.Thread):
    """Run a blocking ``zfs-agent`` call on a thread and close its fd after.

    The fd is one end of a pipe: closing it once ``zfs`` is done is what
    gives the other end its EOF (send) or EPIPE (receive).
    """

    def __init__(
        self, func: Callable[..., None], dataset: str, fd: int, **kwargs: Any
    ) -> None:
        super().__init__(daemon=True)
        self._call = (func, dataset, fd, kwargs)
        self.error: Exception | None = None

    def run(self) -> None:
        func, dataset, fd, kwargs = self._call
        try:
            func(dataset, fd, **kwargs)
        except Exception as e:
            self.error = e
        finally:
            os.close(fd)

    def result(self) -> None:
        self.join()
        if self.error is not None:
            raise self.error


def pipe() -> tuple[int, int]:
    r, w = os.pipe()
    if sys.platform != "win32":
        with suppress(OSError):  # fewer, larger reads and writes; best effort
            fcntl.fcntl(w, fcntl.F_SETPIPE_SZ, PIPE_SIZE)
    return r, w


def write_all(fd: int, data: bytes) -> None:
    """``os.write`` until all of ``data`` is out – a blocking pipe write may
    still come back short when a signal interrupts it."""
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def read_chunk(fd: int, alive: Callable[[], bool]) -> bytes:
    """Read up to ``CHUNK_SIZE`` bytes, but hand over what's there rather
    than wait for a full chunk once the pipe runs dry. Empty at EOF, or once
    ``alive`` turns false – checked every `POLL` seconds, so a stalled
    ``zfs`` can't keep a stopped reader blocked."""
    buf = bytearray()
    while len(buf) < CHUNK_SIZE and alive():
        ready, _, _ = select.select([fd], [], [], POLL)
        if not ready:
            if buf:
                break
            continue
        data = os.read(fd, CHUNK_SIZE - len(buf))
        if not data:
            break
        buf += data
    return bytes(buf)


def slots(buffer: int) -> int:
    """Queue length holding up to ``buffer`` bytes of chunks."""
    return max(1, buffer // CHUNK_SIZE)


def put(q: "queue.Queue[Any]", item: Any, alive: Callable[[], bool]) -> bool:
    """Put ``item`` unless the consumer went away meanwhile."""
    while alive():
        try:
            q.put(item, timeout=POLL)
            return True
        except queue.Full:
            continue
    return False
