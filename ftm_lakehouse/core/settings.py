from pathlib import Path

from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.settings import BaseSettings
from pydantic_settings import SettingsConfigDict

CHECKSUM_ALGORITHM = "sha256"  # never change this! ;)

__version__ = "0.6.1"

SECRETS_DIR = Path("/run/secrets")
"""Docker secrets mount"""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="lakehouse_",
        env_nested_delimiter="__",
        env_file=".env",
        secrets_dir=str(SECRETS_DIR) if SECRETS_DIR.is_dir() else None,
        nested_model_default_partial_update=True,
        extra="ignore",
    )

    uri: str = "data"
    journal_uri: str = "sqlite:///:memory:"
    api_key: str | None = None
    api_secret: str | None = None
    on_zfs: bool = False
    zfs_pool: str | None = None
    """ZFS dataset path the lakehouse's tuned datasets are created under.
    Transport / agent configuration (socket, owner, peer auth) lives in the
    external ``zfs-agent`` package's own ``ZFS_*`` environment."""

    grace_period_days: int = 30
    max_buffer_rows: int = 1_000_000

    journal_pool_size: int = 5
    """Postgres journal connections kept warm between writers
    (``LAKEHOUSE_JOURNAL_POOL_SIZE``). ``0`` pools nothing. It is per dataset: a
    worker writing many datasets holds up to this many idle connections for each
    of them, which is the figure to size against postgres
    ``max_connections``."""

    lock_max_retries: int = 22
    """Retry bound when acquiring the dataset write fence (``.LOCK``). Retry
    ``n`` sleeps ``n + rand(0, 1)`` seconds, so the total wait is roughly
    ``N²/2`` seconds – the default of 22 gives up after ~4.5 minutes; a lock
    left behind by a crashed writer must be released via ``ftm-lakehouse
    operations unlock``."""

    duckdb_memory_limit: str = "8GB"
    duckdb_temp_directory: str | None = None
    duckdb_extension_directory: str | None = None

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
    name: str | None = None
    url: str | None = None
    email: str | None = None


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
    contact: ApiContactSettings = ApiContactSettings()

    # DoS limits at the API boundary.
    query_max_in_values: int = 10_000
    """Maximum number of values per ``in`` / ``not_in`` filter in a single
    query body."""

    query_max_filter_keys: int = 20
    """Maximum number of filter leaves accepted in a single query body."""
