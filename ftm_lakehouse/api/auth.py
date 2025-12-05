"""
https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/

Authorization expects an encrypted bearer token with the dataset and checksum lookup
in the subject ({"sub": "<dataset>/<checksum>"}). Therefore, clients need to be able
to create such tokens (knowing the secret checksum) and handle dataset permissions.

Tokens should have a short expiration (via `exp` property in payload).
"""

from datetime import UTC, datetime, timedelta
from typing import Self

import jwt
from anystore.logging import get_logger
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from ftm_lakehouse.api.util import DEFAULT_ERROR, Context, ensure_path_context
from ftm_lakehouse.core.settings import ApiSettings

settings = ApiSettings()
log = get_logger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/", auto_error=False)


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    dataset: str
    checksum: str

    @classmethod
    def from_sub(cls, sub: str) -> Self:
        dataset, checksum = sub.split("/", 1)
        return cls(dataset=dataset, checksum=checksum)


def create_access_token(dataset: str, checksum: str, exp: int | None = None) -> str:
    expires = datetime.now(UTC) + timedelta(minutes=exp or settings.access_token_expire)
    data = {"sub": f"{dataset}/{checksum}", "exp": expires}
    return jwt.encode(
        data, settings.secret_key, algorithm=settings.access_token_algorithm
    )


def ensure_token_context(token: str) -> Context:
    """Get context from url query argument"""

    if not token:
        log.error("Auth: no token")
        raise DEFAULT_ERROR
    try:
        payload = jwt.decode(
            token,
            settings.secret_key,
            algorithms=[settings.access_token_algorithm],
            verify=True,
        )
        data = TokenData.from_sub(payload["sub"])
        return ensure_path_context(data.dataset, data.checksum)
    except Exception as e:
        log.error(f"Invalid token: `{e}`", token=token)
        raise DEFAULT_ERROR


def ensure_auth_context(token: str = Depends(oauth2_scheme)) -> Context:
    """Get context from Authorization header"""

    return ensure_token_context(token)
