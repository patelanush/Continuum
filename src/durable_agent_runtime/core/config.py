from functools import lru_cache

from pydantic import Field, model_validator
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
    executor_lease_seconds: float = Field(default=20, gt=0)
    executor_heartbeat_seconds: float = Field(default=4, gt=0)
    executor_poll_interval_seconds: float = Field(default=0.5, gt=0)
    recovery_scan_interval_seconds: float = Field(default=1, gt=0)
    mock_payments_url: str = "http://localhost:8001"
    executor_drain_seconds: float = Field(default=30, gt=0)

    @model_validator(mode="after")
    def heartbeat_before_expiry(self) -> "Settings":
        if self.executor_heartbeat_seconds >= self.executor_lease_seconds:
            raise ValueError("executor heartbeat interval must be shorter than lease")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
