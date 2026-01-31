"""
https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/

Authorization expects an encrypted bearer token with a list of allowed methods
and glob prefixes or globs. This keeps the auth logic completely external to
applications that control creating jwt tokens. The api only checks if the
request is allowed to access the given method and path.

This allows a very customizable auth implementation, tokens could be very general:

Allow all:
    methods: *
    prefixes: /

Read only:
    methods: GET,HEAD,OPTIONS
    prefixes: /

Only archive access for all datasets:
    methods: *
    prefixes: /*/archive/

Only tags for given datasets:
    methods: *
    prefixes:
        - /dataset_1/tags
        - /dataset_2/tags

Tokens should have a short expiration (via `exp` property in payload). Methods
and prefixes need to be set explicitly, the defaults no no access.
"""

from datetime import UTC, datetime, timedelta
from fnmatch import fnmatch
from typing import Self

import jwt
from anystore.logging import get_logger
from fastapi import Depends, Request
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from ftm_lakehouse.api.util import DEFAULT_ERROR
from ftm_lakehouse.core.settings import ApiSettings

settings = ApiSettings()
log = get_logger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/", auto_error=False)


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    methods: list[str] = []
    prefixes: list[str] = []

    @classmethod
    def from_payload(cls, payload: dict) -> Self:
        return cls(
            methods=payload.get("methods", []),
            prefixes=payload.get("prefixes", []),
        )

    def allows(self, method: str, path: str) -> bool:
        """Check if this token allows the given method on the given path."""
        if "*" not in self.methods and method.upper() not in self.methods:
            return False
        return any(self._match_prefix(path, prefix) for prefix in self.prefixes)

    @staticmethod
    def _match_prefix(path: str, prefix: str) -> bool:
        """Match path against a prefix pattern.

        Prefixes without glob characters are treated as path prefixes
        (e.g. "/" matches everything, "/dataset_1/tags" matches
        "/dataset_1/tags/foo"). Prefixes with glob characters use fnmatch.
        """
        if "*" in prefix or "?" in prefix:
            return fnmatch(path, prefix)
        return path.startswith(prefix)


def create_access_token(
    methods: list[str] | None = None,
    prefixes: list[str] | None = None,
    sub: str | None = None,
    exp: int | None = None,
) -> str:
    expires = datetime.now(UTC) + timedelta(minutes=exp or settings.access_token_expire)
    data: dict = {"exp": expires}
    if methods:
        data["methods"] = methods
    if prefixes:
        data["prefixes"] = prefixes
    if sub:
        data["sub"] = sub
    return jwt.encode(
        data, settings.secret_key, algorithm=settings.access_token_algorithm
    )


def ensure_token_context(token: str, request: Request) -> TokenData:
    """Decode token and verify it allows the request method and path."""
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
        data = TokenData.from_payload(payload)
        if not data.allows(request.method, request.url.path):
            log.error(
                "Auth: method/path not allowed",
                method=request.method,
                path=request.url.path,
            )
            raise DEFAULT_ERROR
        return data
    except DEFAULT_ERROR.__class__:
        raise
    except Exception as e:
        log.error(f"Invalid token: `{e}`", token=token)
        raise DEFAULT_ERROR


def ensure_auth(request: Request, token: str = Depends(oauth2_scheme)) -> TokenData:
    """Get token data from Authorization header and verify access."""
    return ensure_token_context(token, request)
