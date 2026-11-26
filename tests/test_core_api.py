import httpx
import pytest

from ftm_lakehouse.core.api import LakehouseApi


def _api(uri: str, status: int) -> LakehouseApi:
    api = LakehouseApi(uri)
    api.client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(status))
    )
    return api


def test_ensure_tolerates_405() -> None:
    _api("http://lake:8000/leak", 405).ensure()


@pytest.mark.parametrize(
    "uri,status",
    [
        ("http://lake:8000/leak", 403),
        ("http://lake:8000/leak_2405", 403),
        ("http://lake:4050/leak", 404),
    ],
)
def test_ensure_raises_other_errors(uri: str, status: int) -> None:
    with pytest.raises(httpx.HTTPStatusError):
        _api(uri, status).ensure()
