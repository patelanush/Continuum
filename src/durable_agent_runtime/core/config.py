from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = Field(
        default="postgresql+asyncpg://durable:durable@127.0.0.1:55433/durable"
    )
    kafka_bootstrap_servers: str = "127.0.0.1:19092"
    kafka_consumer_group: str = "continuum-workers-v1"
    outbox_poll_interval: float = 0.5
    outbox_publish_lease_seconds: float = Field(default=30, gt=10)
    executor_lease_seconds: float = Field(default=20, gt=0)
    executor_heartbeat_seconds: float = Field(default=4, gt=0)
    executor_poll_interval_seconds: float = Field(default=0.5, gt=0)
    recovery_scan_interval_seconds: float = Field(default=1, gt=0)
    mock_payments_url: str = "http://localhost:8001"
    executor_drain_seconds: float = Field(default=30, gt=0)
    agent_provider: str = "ollama"
    ollama_base_url: str = "http://host.docker.internal:11434"
    ollama_model: str = "qwen2.5:3b"
    model_timeout_seconds: float = Field(default=90, gt=0)
    model_max_attempts: int = Field(default=3, ge=1)
    agent_max_turns: int = Field(default=8, ge=1)
    coding_agent_max_turns: int = Field(default=12, ge=1)
    sandbox_image: str = "continuum-sandbox:phase6"
    sandbox_volume_prefix: str = "continuum"
    coding_command_timeout_seconds: int = Field(default=30, ge=1, le=300)
    coding_max_file_read_bytes: int = Field(default=32_768, ge=1024, le=1_000_000)
    coding_max_patch_bytes: int = Field(default=32_768, ge=1024, le=1_000_000)
    coding_max_command_output_bytes: int = Field(default=8_192, ge=1024, le=100_000)
    coding_max_search_results: int = Field(default=50, ge=1, le=500)

    @model_validator(mode="after")
    def heartbeat_before_expiry(self) -> "Settings":
        if self.executor_heartbeat_seconds >= self.executor_lease_seconds:
            raise ValueError("executor heartbeat interval must be shorter than lease")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
