"""Lakehouse as http api delegation"""

from functools import cache, wraps
from typing import Callable, Generator, TypeVar

import httpx
from anystore.decorators import error_handler
from anystore.logic.uri import join_uri
from anystore.store.resource import UriResource
from anystore.types import Uri
from anystore.util import mask_uri
from fsspec.config import conf as fsspec_conf

from ftm_lakehouse.core.settings import Settings, __version__

F = TypeVar("F", bound=Callable)

USER_AGENT = f"ftm-lakehouse/{__version__}"

MAX_RETRIES = 5
"""Attempts per api request before the error propagates."""

_default_headers: dict[str, str] = {"User-Agent": USER_AGENT}
_settings = Settings()
if _settings.api_key and _settings.api_secret:
    _default_headers["X-Api-Key"] = _settings.api_key
    _default_headers["X-Api-Secret"] = _settings.api_secret

# Set default headers for all ApiFileSystem (anystore+http[s]) instances
_fsspec_client_kwargs = {"headers": _default_headers}
fsspec_conf.setdefault("anystore+http", {})["client_kwargs"] = _fsspec_client_kwargs
fsspec_conf.setdefault("anystore+https", {})["client_kwargs"] = _fsspec_client_kwargs


@cache
def ensure_api_uri(uri: Uri) -> Uri:
    """Convert http[s]:// URIs to anystore+http[s]:// for ApiFileSystem support."""
    uri_str = str(uri)
    if uri_str.startswith("https://"):
        return f"anystore+{uri_str}"
    if uri_str.startswith("http://"):
        return f"anystore+{uri_str}"
    return uri


class LakehouseApi(UriResource):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not self.is_http:
            raise RuntimeError(f"Lakehouse api uri is not http: `{mask_uri(self.uri)}`")
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout=3600.0 * 6),
            headers=_default_headers,
        )

    def make_url(self, endpoint: str) -> str:
        return join_uri(self.uri, endpoint)

    def make_request(
        self, url: str, method: str = "GET", **kwargs  # noqa: ANN003
    ) -> httpx.Response:
        """Perform one api request, retrying transient failures.

        Raises:
            httpx.HTTPStatusError: On a 4xx, or a 5xx that outlived its retries.
            httpx.HTTPError: On a transport failure that outlived its retries.
        """

        def _request() -> httpx.Response:
            res = self.client.request(method, url, **kwargs)
            if res.status_code >= 500:
                res.raise_for_status()
            return res

        res: httpx.Response = error_handler(max_retries=MAX_RETRIES, do_raise=True)(
            _request
        )()
        res.raise_for_status()
        return res

    def stream_request(
        self, url: str, method: str = "GET", **kwargs  # noqa: ANN003
    ) -> Generator[str, None, None]:
        """Stream a request line by line."""
        with self.client.stream(method, url, **kwargs) as stream:
            stream.raise_for_status()
            yield from stream.iter_lines()

    def ensure(self) -> None:
        """Trigger ensure zfs dataset endpoint"""
        url = self.make_url("_api/ensure")
        self.make_request(url, "POST")


@cache
def get_api(uri: Uri) -> LakehouseApi | None:
    try:
        return LakehouseApi(uri)
    except RuntimeError:
        return


class LakehouseApiMixin:
    def __init__(self, uri: Uri) -> None:
        self.__api = get_api(uri)

    @property
    def _is_api(self) -> bool:
        """Check if the backend is the HTTP API."""
        return self.__api is not None

    @property
    def _api(self) -> LakehouseApi:
        """Return the API client. Raises if not in API mode."""
        if self.__api is None:
            raise RuntimeError(f"`{type(self).__name__}._api` is not available")
        return self.__api


def no_api(method: F) -> F:
    """Decorator for methods that cannot be called when the backend is the HTTP
    API."""

    @wraps(method)
    def wrapper(
        self: "LakehouseApiMixin", *args, **kwargs
    ):  # noqa: ANN002,ANN003,ANN202
        if self._is_api:
            raise RuntimeError(
                f"`{type(self).__name__}.{method.__name__}` is not available in API mode"
            )
        return method(self, *args, **kwargs)

    return wrapper  # type: ignore[return-value]
