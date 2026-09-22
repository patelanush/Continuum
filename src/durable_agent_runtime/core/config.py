from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = Field(
        default="postgresql+asyncpg://durable:durable@localhost:55433/durable"
    )
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_consumer_group: str = "continuum-workers-v1"
    outbox_poll_interval: float = 0.5


@lru_cache
def get_settings() -> Settings:
    return Settings()
