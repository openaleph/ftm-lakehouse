from pathlib import Path
from tempfile import gettempdir

from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.settings import BaseSettings
from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict

from ftm_lakehouse.util import parse_byte_size

CHECKSUM_ALGORITHM = "sha256"  # never change this! ;)

__version__ = "0.9.0"

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
    """ZFS dataset path the lakehouse's datasets are created under"""

    grace_period_days: int = 30
    max_buffer_rows: int = 1_000_000

    journal_drain_rows: int = 1_000_000
    """Rows per Arrow table a journal flush hands to the parquet store"""

    journal_pool_size: int = 5
    """Postgres journal connections (adbc)"""

    lock_max_retries: int = 10
    """Retry bound for every wait on a dataset lock: acquiring ``.LOCK`` or
    ``.LOCK-MERGE``. A lock left behind can be released via ``ftm-lakehouse
    maintenance unlock``."""

    duckdb_memory_limit: str = "8GB"
    """DuckDB's memory budget as a byte size (``8GB``, ``512MiB``) – it is split
    between the workers, so a share of RAM such as ``80%`` is rejected."""

    workers: int = Field(default=1, ge=1)
    """Processes ``merge`` and the export sweep fan their partitions out to
    (``LAKEHOUSE_WORKERS``); ``1`` runs in-process. `duckdb_memory_limit` and
    the CPU threads are split between them. A sweep uses at most one worker per
    ``(shard, bucket)`` pair."""

    duckdb_temp_directory: str | None = Field(
        default_factory=lambda: str(Path(gettempdir()) / "duckdb")
    )
    """Where DuckDB spills a query that outgrows `duckdb_memory_limit` – each
    DuckDB instance into its own subdirectory, removed on close."""

    duckdb_extension_directory: str | None = None

    public_url_prefix: str | None = None

    @field_validator("duckdb_memory_limit")
    @classmethod
    def _byte_size(cls, value: str) -> str:
        parse_byte_size(value)
        return value

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
