"""Pydantic models for YAML-backed job configurations."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ctx_ctr.constants import DEFAULT_FLINK_KAFKA_CONNECTOR_JAR
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
    DEAD_LETTER_TOPIC,
    EVENT_TOPIC,
    IMPRESSION_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    POSTGRES_DSN,
    REDIS_URL,
)


class SeedValuesJobConfig(BaseModel):
    """Configuration for the seed-values job."""

    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class ResetValuesJobConfig(BaseModel):
    """Configuration for the reset-values job."""

    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class CleanTopicsJobConfig(BaseModel):
    """Configuration for the Kafka topic-cleanup job."""

    only: list[str] | None = None
    dry_run: bool = False
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    event_topic: str = EVENT_TOPIC
    dead_letter_topic: str = DEAD_LETTER_TOPIC

    model_config = ConfigDict(frozen=True)

    @property
    def topic_names(self) -> list[str]:
        """Return selected topic names."""

        return self.only or [
            self.impression_topic,
            self.click_topic,
            self.event_topic,
            self.dead_letter_topic,
        ]


class ProduceEventsJobConfig(BaseModel):
    """Configuration for the simulated event producer job."""

    impressions: int = Field(default=100, gt=0)
    events_per_second: float = Field(default=20.0, ge=0)
    random_seed: int = 42
    log_every: int = Field(default=10, ge=0)
    also_unified: bool = False
    dry_run: bool = False
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    event_topic: str = EVENT_TOPIC

    model_config = ConfigDict(frozen=True)


class RunRealtimeCtrJobConfig(BaseModel):
    """Configuration for the realtime CTR PyFlink job."""

    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    dead_letter_topic: str = DEAD_LETTER_TOPIC
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    redis_url: str = REDIS_URL
    consumer_group: str = "ctx-ctr-flink-realtime"
    parallelism: int = Field(default=1, gt=0)
    checkpoint_interval_ms: int = Field(default=10000, ge=0)
    kafka_connector_jar: str = DEFAULT_FLINK_KAFKA_CONNECTOR_JAR
    log_every: int = Field(default=100, ge=0)

    model_config = ConfigDict(frozen=True)


class RunWeightUpdateJobConfig(BaseModel):
    """Configuration for the periodic weight-update job."""

    redis_url: str = REDIS_URL
    postgres_dsn: str = POSTGRES_DSN
    interval_seconds: int = Field(default=3600, gt=0)
    once: bool = False
    learning_rate: float = Field(default=0.25, gt=0)
    evidence_smoothing: float = Field(default=1000.0, ge=0)
    ridge: float = Field(default=0.01, ge=0)
    max_delta: float = Field(default=0.25, gt=0)
    min_trusted_buckets: int = Field(default=1, ge=1)
    snapshot_name_prefix: str = Field(default="flink_weight_update", min_length=1)
    baseline_update: bool = True
    baseline_learning_rate: float = Field(default=0.10, gt=0)
    baseline_evidence_smoothing: float = Field(default=5000.0, ge=0)
    baseline_max_delta: float = Field(default=0.10, gt=0)
    baseline_min_impressions: int = Field(default=1000, ge=1)
    baseline_max_ci_width: float = Field(default=0.02, gt=0)
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class WatchRedisValuesJobConfig(BaseModel):
    """Configuration for the Redis value inspection job."""

    redis_url: str = REDIS_URL
    pattern: str = "*"
    limit: int = Field(default=200, gt=0)
    watch: bool = False
    interval_seconds: float = Field(default=2.0, gt=0)
    pretty_json: bool = True
    only_bucket: bool = False
    ad_category: str | None = None
    publisher_domain: str | None = None
    conversation_category: str | None = None

    model_config = ConfigDict(frozen=True)
