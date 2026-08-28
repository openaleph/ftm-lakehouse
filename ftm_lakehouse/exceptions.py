class ImproperlyConfigured(BaseException):
    pass


class BufferFullError(RuntimeError):
    """Raised when an `EntityBuffer` hits its row cap without being flushed.

    The cap defends against unbounded memory growth when a caller forgets
    to flush (or chooses a ``bulk_size`` larger than
    `Settings.max_buffer_rows`). Catch this, call ``flush_table()``
    + ``write_batches`` (or whatever drains the buffer), then retry.
    """
