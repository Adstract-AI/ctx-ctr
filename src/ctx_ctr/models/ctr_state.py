"""Typed runtime CTR state and model-weight payloads."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ctx_ctr.constants import REDIS_CTR_KEY_PREFIX

JsonPayload = dict[str, object]


class CtrBucketKey(BaseModel):
    """Stable key for one contextual CTR bucket."""

    ad_category: str
    publisher_domain: str
    conversation_category: str

    model_config = ConfigDict(frozen=True)

    @property
    def redis_key(self) -> str:
        """Return the Redis key for this bucket."""

        return (
            f"{REDIS_CTR_KEY_PREFIX}:"
            f"{self.ad_category}:"
            f"{self.publisher_domain}:"
            f"{self.conversation_category}"
        )

    @property
    def kafka_key(self) -> str:
        """Return the Kafka partitioning key for this bucket."""

        return f"{self.ad_category}:{self.publisher_domain}:{self.conversation_category}"


class CtrBucketStatistic(BaseModel):
    """Bayesian runtime CTR state for one contextual bucket."""

    ad_category: str
    publisher_domain: str
    conversation_category: str
    impressions: int = Field(ge=0)
    clicks: int = Field(ge=0)
    alpha_prior: float = Field(gt=0)
    beta_prior: float = Field(gt=0)
    alpha_posterior: float = Field(gt=0)
    beta_posterior: float = Field(gt=0)
    ctr: float = Field(ge=0, le=1)
    variance: float = Field(ge=0)
    ci_low: float = Field(ge=0, le=1)
    ci_high: float = Field(ge=0, le=1)
    trusted: bool

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def validate_counts_and_interval(self) -> "CtrBucketStatistic":
        """Validate count and confidence interval invariants."""

        if self.clicks > self.impressions:
            raise ValueError("clicks cannot exceed impressions")
        if self.ci_low > self.ci_high:
            raise ValueError("ci_low cannot exceed ci_high")
        return self

    @property
    def bucket_key(self) -> CtrBucketKey:
        """Return the structured bucket key."""

        return CtrBucketKey(
            ad_category=self.ad_category,
            publisher_domain=self.publisher_domain,
            conversation_category=self.conversation_category,
        )

    @property
    def redis_key(self) -> str:
        """Return the Redis key for this bucket."""

        return self.bucket_key.redis_key

    def redis_payload(self) -> JsonPayload:
        """Return the Redis payload compatible with seeded bucket statistics."""

        return {
            "ad_category": self.ad_category,
            "publisher_domain": self.publisher_domain,
            "conversation_category": self.conversation_category,
            "impressions": self.impressions,
            "clicks": self.clicks,
            "alpha_prior": self.alpha_prior,
            "beta_prior": self.beta_prior,
            "alpha_posterior": self.alpha_posterior,
            "beta_posterior": self.beta_posterior,
            "ctr": self.ctr,
            "variance": self.variance,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "trusted": self.trusted,
        }


class CtrModelWeights(BaseModel):
    """Feature-family weights used for CTR prior construction."""

    w_ad: dict[str, float]
    w_dom: dict[str, float]
    w_ctx: dict[str, float]

    model_config = ConfigDict(frozen=True)


class CtrModelMetrics(BaseModel):
    """Model metrics required by runtime CTR processing."""

    baseline_ctr: float = Field(gt=0, lt=1)
    global_prior_strength: float = Field(gt=0)
    triplet_prior_strength: float = Field(gt=0)
    family_prior_strengths: dict[str, float]

    model_config = ConfigDict(frozen=True)


class CtrModelSnapshot(BaseModel):
    """Current CTR model payload stored in Redis."""

    snapshot_name: str
    w0: float
    weights: CtrModelWeights
    metrics: CtrModelMetrics

    model_config = ConfigDict(frozen=True)

    def redis_payload(self) -> JsonPayload:
        """Return the Redis-compatible current model payload."""

        return {
            "snapshot_name": self.snapshot_name,
            "w0": self.w0,
            "weights": self.weights.model_dump(),
            "metrics": self.metrics.model_dump(),
        }


class DeadLetterPayload(BaseModel):
    """Dead-letter payload for events rejected by realtime CTR processing."""

    reason: str
    event: JsonPayload

    model_config = ConfigDict(frozen=True)
