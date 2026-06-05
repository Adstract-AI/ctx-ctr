"""Runtime settings for local development and service access."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ctx_ctr.constants import (
    DEFAULT_CLICK_TOPIC,
    DEFAULT_DEAD_LETTER_TOPIC,
    DEFAULT_EVENT_TOPIC,
    DEFAULT_FLINK_REST_URL,
    DEFAULT_IMPRESSION_TOPIC,
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_POSTGRES_DSN,
    DEFAULT_REDIS_URL,
    DEFAULT_SPARK_MASTER_URL,
)


class RuntimeSettings(BaseSettings):
    """Connection settings used by local jobs, services, and adapters."""

    kafka_bootstrap_servers: str = Field(default=DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    redis_url: str = Field(default=DEFAULT_REDIS_URL)
    postgres_dsn: str = Field(default=DEFAULT_POSTGRES_DSN)
    spark_master_url: str = Field(default=DEFAULT_SPARK_MASTER_URL)
    flink_rest_url: str = Field(default=DEFAULT_FLINK_REST_URL)
    impression_topic: str = Field(default=DEFAULT_IMPRESSION_TOPIC)
    click_topic: str = Field(default=DEFAULT_CLICK_TOPIC)
    event_topic: str = Field(default=DEFAULT_EVENT_TOPIC)
    dead_letter_topic: str = Field(default=DEFAULT_DEAD_LETTER_TOPIC)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def load_settings() -> RuntimeSettings:
    """Load runtime settings from environment variables and local `.env` files."""

    return RuntimeSettings()
