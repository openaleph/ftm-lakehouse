from anystore.exceptions import DoesNotExist
from anystore.io import smart_read
from anystore.settings import BaseSettings
from anystore.types import HttpUrlStr
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="lakehouse_",
        env_nested_delimiter="__",
        env_file=".env",
        nested_model_default_partial_update=True,
        extra="ignore",
    )

    uri: str = "data"
    public_url_prefix: str | None = None
    journal_uri: str = "sqlite:///data/journal.db"


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
    auth_required: bool = True

    title: str = "FollowTheMoney Data Lakehouse Api"
    description: str = get_api_doc()
    contact: ApiContactSettings | None = None

    allowed_origins: list[HttpUrlStr] = ["http://localhost:3000"]
