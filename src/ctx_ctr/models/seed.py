"""Typed seed data models."""

from __future__ import annotations

from datetime import datetime
from typing import TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ctx_ctr.constants import REDIS_CTR_KEY_PREFIX

SeedMetadataValue: TypeAlias = (
    str | int | float | bool | None | list[str] | dict[str, float]
)
SeedMetadata: TypeAlias = dict[str, SeedMetadataValue]
SeedWeightPayload: TypeAlias = dict[str, dict[str, float]]
SeedModelPayloadValue: TypeAlias = str | float | SeedWeightPayload | SeedMetadata
SeedModelPayload: TypeAlias = dict[str, SeedModelPayloadValue]


class SeedBucketStatistic(BaseModel):
    """Initial Bayesian CTR state for one contextual bucket."""

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
    def validate_counts_and_interval(self) -> "SeedBucketStatistic":
        """Validate CTR count and confidence interval invariants."""

        if self.clicks > self.impressions:
            raise ValueError("clicks cannot exceed impressions")
        if self.ci_low > self.ci_high:
            raise ValueError("ci_low cannot exceed ci_high")
        return self

    @property
    def redis_key(self) -> str:
        """Return the feature-store key for this bucket."""

        return (
            f"{REDIS_CTR_KEY_PREFIX}:"
            f"{self.ad_category}:"
            f"{self.publisher_domain}:"
            f"{self.conversation_category}"
        )

    def redis_payload(self) -> SeedMetadata:
        """Return a JSON-compatible feature-store payload."""

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


class SeedWeights(BaseModel):
    """Centered model-weight families used for CTR prior construction."""

    w_ad: dict[str, float]
    w_dom: dict[str, float]
    w_ctx: dict[str, float]

    model_config = ConfigDict(frozen=True)

    @field_validator("w_ad", "w_dom", "w_ctx")
    @classmethod
    def validate_centered_family(cls, weights: dict[str, float]) -> dict[str, float]:
        """Ensure every weight family is non-empty and centered."""

        if not weights:
            raise ValueError("weight family cannot be empty")
        mean = sum(weights.values()) / len(weights)
        if abs(mean) > 1e-12:
            raise ValueError("weight family must be centered")
        return weights


class SeedModelMetrics(BaseModel):
    """Metrics describing the seeded model snapshot."""

    baseline_ctr: float = Field(gt=0, lt=1)
    global_prior_strength: float = Field(gt=0)
    triplet_prior_strength: float = Field(gt=0)
    family_prior_strengths: dict[str, float]

    model_config = ConfigDict(frozen=True)

    @field_validator("family_prior_strengths")
    @classmethod
    def validate_family_prior_strengths(cls, strengths: dict[str, float]) -> dict[str, float]:
        """Ensure all feature-family prior strengths are present and positive."""

        expected_families = {"ad", "domain", "context"}
        if set(strengths) != expected_families:
            raise ValueError("family_prior_strengths must contain ad, domain, and context")
        if any(strength <= 0 for strength in strengths.values()):
            raise ValueError("family prior strengths must be positive")
        return strengths


class SeedModelSnapshot(BaseModel):
    """Initial model weights used to construct CTR priors."""

    snapshot_name: str
    w0: float
    weights: SeedWeights
    metrics: SeedModelMetrics

    model_config = ConfigDict(frozen=True)

    def redis_payload(self) -> SeedModelPayload:
        """Return a JSON-compatible current-weight payload."""

        return {
            "snapshot_name": self.snapshot_name,
            "w0": self.w0,
            "weights": self.weights.model_dump(),
            "metrics": self.metrics.model_dump(),
        }


class SeedRunSummary(BaseModel):
    """Metadata describing a deterministic seed run."""

    summary_name: str
    config: SeedMetadata
    metrics: SeedMetadata
    artifact_uri: str | None = None

    model_config = ConfigDict(frozen=True)


class SeedDataset(BaseModel):
    """Complete deterministic dataset used by the seeder."""

    generated_at: datetime
    bucket_statistics: list[SeedBucketStatistic]
    model_snapshots: list[SeedModelSnapshot]
    run_summaries: list[SeedRunSummary]

    model_config = ConfigDict(frozen=True, protected_namespaces=())

    @model_validator(mode="after")
    def validate_dataset(self) -> "SeedDataset":
        """Ensure the seed dataset contains unique bucket and snapshot records."""

        bucket_keys = {
            (
                bucket.ad_category,
                bucket.publisher_domain,
                bucket.conversation_category,
            )
            for bucket in self.bucket_statistics
        }
        if len(bucket_keys) != len(self.bucket_statistics):
            raise ValueError("bucket statistics contain duplicate bucket keys")

        snapshot_names = {snapshot.snapshot_name for snapshot in self.model_snapshots}
        if len(snapshot_names) != len(self.model_snapshots):
            raise ValueError("model snapshots contain duplicate names")

        return self

    def summary(self) -> SeedMetadata:
        """Return deterministic counts and aggregate metrics for reporting."""

        impressions = sum(bucket.impressions for bucket in self.bucket_statistics)
        clicks = sum(bucket.clicks for bucket in self.bucket_statistics)
        return {
            "generated_at": self.generated_at.isoformat(),
            "bucket_statistics": len(self.bucket_statistics),
            "model_snapshots": len(self.model_snapshots),
            "run_summaries": len(self.run_summaries),
            "total_impressions": impressions,
            "total_clicks": clicks,
        }
