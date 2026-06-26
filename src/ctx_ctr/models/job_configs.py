"""Pydantic models for YAML-backed job configurations."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ctx_ctr.constants import (
    DEFAULT_CTR_TRUST_MAX_CI_WIDTH,
    DEFAULT_CTR_TRUST_MAX_VARIANCE,
    DEFAULT_CTR_TRUST_MIN_IMPRESSIONS,
    DEFAULT_CTR_TRUST_Z_SCORE,
    DEFAULT_FLINK_KAFKA_CONNECTOR_JAR,
    DEFAULT_WEIGHT_UPDATE_BASELINE_MAX_CI_WIDTH,
    DEFAULT_WEIGHT_UPDATE_EVIDENCE_SMOOTHING,
    DEFAULT_WEIGHT_UPDATE_INTERVAL_SECONDS,
    DEFAULT_WEIGHT_UPDATE_LEARNING_RATE,
    DEFAULT_WEIGHT_UPDATE_MAX_DELTA,
    DEFAULT_WEIGHT_UPDATE_MAX_FEATURE_CI_WIDTH,
    DEFAULT_WEIGHT_UPDATE_MIN_FEATURE_IMPRESSIONS,
    DEFAULT_WEIGHT_UPDATE_RIDGE,
)
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
    DEAD_LETTER_TOPIC,
    EVENT_TOPIC,
    IMPRESSION_TOPIC,
)


class SeedValuesJobConfig(BaseModel):
    """Configuration for the seed-values job."""

    dry_run: bool = False
    baseline_prior_mean: float = Field(default=0.02, gt=0, lt=1)
    triplet_prior_strength: float = Field(default=100.0, gt=0)
    global_prior_strength: float = Field(default=500.0, gt=0)
    ad_prior_strength: float = Field(default=200.0, gt=0)
    domain_prior_strength: float = Field(default=200.0, gt=0)
    context_prior_strength: float = Field(default=150.0, gt=0)

    model_config = ConfigDict(frozen=True)


class ResetValuesJobConfig(BaseModel):
    """Configuration for the reset-values job."""

    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class CleanTopicsJobConfig(BaseModel):
    """Configuration for the Kafka topic-cleanup job."""

    only: list[str] | None = None
    dry_run: bool = False
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
    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    event_topic: str = EVENT_TOPIC

    model_config = ConfigDict(frozen=True)


class RunRealtimeCtrJobConfig(BaseModel):
    """Configuration for the realtime CTR PyFlink job."""

    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    dead_letter_topic: str = DEAD_LETTER_TOPIC
    consumer_group: str = "ctx-ctr-flink-realtime"
    parallelism: int = Field(default=1, gt=0)
    checkpoint_interval_ms: int = Field(default=10000, ge=0)
    kafka_connector_jar: str = DEFAULT_FLINK_KAFKA_CONNECTOR_JAR
    log_every: int = Field(default=100, ge=0)
    trust_z_score: float = Field(default=DEFAULT_CTR_TRUST_Z_SCORE, gt=0)
    trust_min_impressions: int = Field(default=DEFAULT_CTR_TRUST_MIN_IMPRESSIONS, ge=0)
    trust_max_variance: float = Field(default=DEFAULT_CTR_TRUST_MAX_VARIANCE, gt=0)
    trust_max_ci_width: float = Field(default=DEFAULT_CTR_TRUST_MAX_CI_WIDTH, gt=0, le=1)

    model_config = ConfigDict(frozen=True)


class RunWeightUpdateJobConfig(BaseModel):
    """Configuration for the periodic weight-update job."""

    interval_seconds: int = Field(default=DEFAULT_WEIGHT_UPDATE_INTERVAL_SECONDS, gt=0)
    once: bool = False
    learning_rate: float = Field(default=DEFAULT_WEIGHT_UPDATE_LEARNING_RATE, gt=0)
    evidence_smoothing: float = Field(default=DEFAULT_WEIGHT_UPDATE_EVIDENCE_SMOOTHING, ge=0)
    ridge: float = Field(default=DEFAULT_WEIGHT_UPDATE_RIDGE, ge=0)
    max_delta: float = Field(default=DEFAULT_WEIGHT_UPDATE_MAX_DELTA, gt=0)
    min_feature_impressions: int = Field(
        default=DEFAULT_WEIGHT_UPDATE_MIN_FEATURE_IMPRESSIONS,
        ge=1,
    )
    max_feature_ci_width: float = Field(
        default=DEFAULT_WEIGHT_UPDATE_MAX_FEATURE_CI_WIDTH,
        gt=0,
    )
    snapshot_name_prefix: str = Field(default="flink_weight_update", min_length=1)
    baseline_update: bool = True
    baseline_max_ci_width: float = Field(
        default=DEFAULT_WEIGHT_UPDATE_BASELINE_MAX_CI_WIDTH,
        gt=0,
    )
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class RunStreamingWeightUpdateJobConfig(BaseModel):
    """Configuration for the Flink-native streaming weight-update job."""

    impression_topic: str = IMPRESSION_TOPIC
    click_topic: str = CLICK_TOPIC
    consumer_group: str = "ctx-ctr-flink-streaming-weight-update"
    parallelism: int = Field(default=1, gt=0)
    checkpoint_interval_ms: int = Field(default=10000, ge=0)
    kafka_connector_jar: str = DEFAULT_FLINK_KAFKA_CONNECTOR_JAR
    interval_seconds: int = Field(default=DEFAULT_WEIGHT_UPDATE_INTERVAL_SECONDS, gt=0)
    learning_rate: float = Field(default=DEFAULT_WEIGHT_UPDATE_LEARNING_RATE, gt=0)
    evidence_smoothing: float = Field(default=DEFAULT_WEIGHT_UPDATE_EVIDENCE_SMOOTHING, ge=0)
    ridge: float = Field(default=DEFAULT_WEIGHT_UPDATE_RIDGE, ge=0)
    max_delta: float = Field(default=DEFAULT_WEIGHT_UPDATE_MAX_DELTA, gt=0)
    min_feature_impressions: int = Field(
        default=DEFAULT_WEIGHT_UPDATE_MIN_FEATURE_IMPRESSIONS,
        ge=1,
    )
    max_feature_ci_width: float = Field(
        default=DEFAULT_WEIGHT_UPDATE_MAX_FEATURE_CI_WIDTH,
        gt=0,
    )
    snapshot_name_prefix: str = Field(default="flink_streaming_weight_update", min_length=1)
    baseline_update: bool = True
    baseline_max_ci_width: float = Field(
        default=DEFAULT_WEIGHT_UPDATE_BASELINE_MAX_CI_WIDTH,
        gt=0,
    )
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class PersistRedisBucketsJobConfig(BaseModel):
    """Configuration for Redis-to-Postgres bucket persistence."""

    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class WatchRedisValuesJobConfig(BaseModel):
    """Configuration for the Redis value inspection job."""

    pattern: str = "*"
    limit: int = Field(default=200, gt=0)
    watch: bool = False
    interval_seconds: float = Field(default=2.0, gt=0)
    pretty_json: bool = True
    ad_category: str | None = None
    publisher_domain: str | None = None
    conversation_category: str | None = None

    model_config = ConfigDict(frozen=True)


class RunExperimentJobConfig(BaseModel):
    """Configuration for the experiment runner job."""

    experiment_name: str = Field(
        default="local_smoke",
        min_length=1,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    output_dir: str = "experiments/results"
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)
