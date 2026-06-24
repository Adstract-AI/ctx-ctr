"""Typed models for the Task 2 weight-update flow."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot


class RedisBucketScanResult(BaseModel):
    """Bucket payloads parsed from Redis along with scan counts."""

    scanned_key_count: int = Field(ge=0)
    valid_bucket_count: int = Field(ge=0)
    invalid_bucket_count: int = Field(ge=0)
    buckets: list[SeedBucketStatistic] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)


class WeightUpdateRunConfig(BaseModel):
    """Runtime controls for one weight recalibration pass."""

    learning_rate: float = Field(gt=0)
    evidence_smoothing: float = Field(ge=0)
    ridge: float = Field(ge=0)
    max_delta: float = Field(gt=0)
    min_trusted_buckets: int = Field(ge=1)
    snapshot_name_prefix: str = Field(min_length=1)
    baseline_update: bool = True
    baseline_learning_rate: float = Field(default=0.10, gt=0)
    baseline_evidence_smoothing: float = Field(default=5000.0, ge=0)
    baseline_max_delta: float = Field(default=0.10, gt=0)
    baseline_min_impressions: int = Field(default=1000, ge=1)
    baseline_max_ci_width: float = Field(default=0.02, gt=0)
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class WeightUpdateRunMetrics(BaseModel):
    """Metrics captured for one weight recalibration pass."""

    input_bucket_count: int = Field(ge=0)
    valid_bucket_count: int = Field(ge=0)
    invalid_bucket_count: int = Field(ge=0)
    trusted_bucket_count: int = Field(ge=0)
    skipped_bucket_count: int = Field(ge=0)
    unknown_feature_count: int = Field(ge=0)
    max_absolute_weight_delta: float = Field(ge=0)
    learning_rate: float = Field(gt=0)
    evidence_smoothing: float = Field(ge=0)
    ridge: float = Field(ge=0)
    max_delta: float = Field(gt=0)
    dry_run: bool
    w0_unchanged: bool
    old_w0: float
    new_w0: float
    baseline_delta: float
    baseline_update_enabled: bool
    baseline_update_applied: bool
    aggregate_impressions: int = Field(ge=0)
    aggregate_clicks: int = Field(ge=0)
    aggregate_observed_ctr: float = Field(ge=0, le=1)
    posterior_baseline_ctr: float = Field(ge=0, le=1)
    baseline_ci_low: float = Field(ge=0, le=1)
    baseline_ci_high: float = Field(ge=0, le=1)
    baseline_ci_width: float = Field(ge=0, le=1)
    baseline_guard_reason: str
    valid_baseline_bucket_count: int = Field(ge=0)
    invalid_baseline_bucket_count: int = Field(ge=0)

    model_config = ConfigDict(frozen=True)


class WeightUpdateResult(BaseModel):
    """Outcome of one Task 2 recalibration pass."""

    accepted: bool
    dry_run: bool
    model_snapshot: SeedModelSnapshot
    metrics: WeightUpdateRunMetrics
    redis_write_applied: bool
    postgres_write_applied: bool
    snapshot_name: str | None = None
    postgres_snapshot_id: int | None = None
    skipped_reason: str | None = None

    model_config = ConfigDict(frozen=True, protected_namespaces=())
