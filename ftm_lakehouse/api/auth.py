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
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from ftm_lakehouse.core.settings import ApiSettings

UNAUTHORIZED = HTTPException(401, headers={"WWW-Authenticate": "Bearer"})
FORBIDDEN = HTTPException(403)

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


SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
TOKEN_ALL_ACCESS = TokenData(methods=["*"], prefixes=["/"])
TOKEN_READONLY = TokenData(methods=list(SAFE_METHODS), prefixes=["/"])


def validate_token(token: str | None, method: str, path: str) -> TokenData:
    """Decode a JWT token and verify it grants access to the given method/path.

    Returns TokenData on success, raises UNAUTHORIZED (401) or FORBIDDEN (403).
    """
    if not token:
        log.error("Auth: no token")
        raise UNAUTHORIZED
    try:
        payload = jwt.decode(
            token,
            settings.secret_key,
            algorithms=[settings.access_token_algorithm],
            verify=True,
        )
    except Exception as e:
        log.error(f"Auth: invalid token: `{e}`")
        raise UNAUTHORIZED
    data = TokenData.from_payload(payload)
    if not data.allows(method, path):
        log.error("Auth: method/path not allowed", method=method, path=path)
        raise FORBIDDEN
    return data


def ensure_auth(request: Request, token: str = Depends(oauth2_scheme)) -> TokenData:
    """Get token data from Authorization header and verify access.

    If auth is not required, allow read-only (GET, HEAD, OPTIONS) requests
    without a token. Write requests are always rejected in public mode.
    """
    if not settings.auth_enabled:
        return TOKEN_ALL_ACCESS
    if not settings.auth_required:
        if request.method.upper() in SAFE_METHODS:
            return TOKEN_READONLY
        raise FORBIDDEN
    return validate_token(token, request.method, request.url.path)


auth_router = APIRouter(prefix="/_auth", tags=["auth"])


@auth_router.get("/validate")
def validate_token_endpoint(
    token: str = Depends(oauth2_scheme),
    x_original_uri: str = Header("/"),  # noqa: B008
    x_original_method: str = Header("GET"),  # noqa: B008
) -> Response:
    """Validate a token for nginx auth_request subrequests.

    nginx sends the original request's Authorization header along with
    X-Original-URI and X-Original-Method so we can check permissions
    against the actual request being proxied.

    Returns 200 on success, 401 for missing/invalid token, 403 if the
    token doesn't grant access to the requested method/path.

    Example nginx configuration::

        location / {
            auth_request /_auth/validate;
            proxy_pass http://api:8000;
        }

        location = /_auth/validate {
            internal;
            proxy_pass http://api:8000/_auth/validate;
            proxy_pass_request_body off;
            proxy_set_header Content-Length "";
            proxy_set_header X-Original-URI $request_uri;
            proxy_set_header X-Original-Method $request_method;
        }
    """
    if not settings.auth_enabled:
        return Response(status_code=200)
    if not settings.auth_required and not token:
        if x_original_method.upper() in SAFE_METHODS:
            return Response(status_code=200)
        raise FORBIDDEN
    validate_token(token, x_original_method, x_original_uri)
    return Response(status_code=200)
