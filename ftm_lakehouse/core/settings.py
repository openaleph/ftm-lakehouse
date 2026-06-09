from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.settings import BaseSettings
from pydantic_settings import SettingsConfigDict

CHECKSUM_ALGORITHM = "sha256"  # never change this! ;)

__version__ = "0.4.0"


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
    api_secret: str | None = None
    on_zfs: bool = False
    zfs_pool: str | None = None
    zfs_socket: str | None = None
    zfs_owner: str | None = None
    zfs_allowed_uid: int | None = None

    entity_shards: int = 0
    grace_period_days: int = 30
    max_buffer_rows: int = 1_000_000

    lock_max_retries: int = 22
    """Retry bound when acquiring the dataset write fence (``.LOCK``). Retry
    ``n`` sleeps ``n + rand(0, 1)`` seconds, so the total wait is roughly
    ``N²/2`` seconds – the default of 22 gives up after ~4.5 minutes, just
    inside a 300s reverse-proxy read timeout. On exhaustion the writer raises
    ``RuntimeError`` instead of waiting forever; a lock left behind by a
    crashed writer must be released via ``ftm-lakehouse operations unlock``."""

    duckdb_memory_limit: str = "4GB"
    duckdb_temp_directory: str | None = None

    public_url_prefix: str | None = None

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

    title: str = "FollowTheMoney Data Lakehouse Api"
    description: str = get_api_doc()
    contact: ApiContactSettings | None = None

    static_headers: dict[str, str] = {}

    # DoS limits at the API boundary.
    max_entity_ids: int = 10_000
    """Maximum number of ``entity_ids`` accepted in a single query body."""

    max_filter_keys: int = 20
    """Maximum number of ftmq filter keys accepted in a single query body."""
