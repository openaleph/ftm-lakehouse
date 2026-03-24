import tempfile

from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.settings import BaseSettings
from anystore.types import HttpUrlStr
from pydantic_settings import SettingsConfigDict

CHECKSUM_ALGORITHM = "sha256"  # never change this! ;)

__version__ = "0.3.0"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="lakehouse_",
        env_nested_delimiter="__",
        env_file=".env",
        secrets_dir="/run/secrets",
        nested_model_default_partial_update=True,
        extra="ignore",
    )

    uri: str = "data"
    journal_uri: str = "sqlite:///:memory:"
    api_key: str | None = None
    on_zfs: bool = False
    zfs_pool: str | None = None
    zfs_socket: str | None = None
    zfs_owner: str | None = None

    tmp_dir: str = tempfile.gettempdir()

    public_url_prefix: str | None = None
    archive_url_expire: int = 900  # seconds (15 minutes)

    @property
    def api_mode(self) -> bool:
        return self.uri.startswith("http")

    @property
    def resolved_journal_uri(self) -> str:
        if self.api_mode:
            # force journal uri to use api as well
            return self.uri
        return self.journal_uri


class ApiContactSettings(BaseSettings):
    name: str | None
    url: str | None
    email: str | None


def get_api_doc() -> str:
    try:
        return smart_read("./README.md", "r")
    except DoesNotExist:
        return ""


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="lakehouse_api_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    secret_key: str = "change-for-production"
    access_token_expire: int = 5  # minutes
    access_token_algorithm: str = "HS256"
    auth_enabled: bool = True  # if disabled, trust reverse proxy!
    auth_required: bool = True

    title: str = "FollowTheMoney Data Lakehouse Api"
    description: str = get_api_doc()
    contact: ApiContactSettings | None = None

    allowed_origins: list[HttpUrlStr] = ["http://localhost:3000"]

    static_headers: dict[str, str] = {}
