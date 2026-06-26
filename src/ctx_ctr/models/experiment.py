"""Typed models for experiment definitions and results."""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

JsonValue: TypeAlias = Any
JsonObject: TypeAlias = dict[str, JsonValue]


class ExperimentTrafficConfig(BaseModel):
    """Traffic generation settings for one experiment run."""

    enabled: bool = True
    impressions: int = Field(default=1000, gt=0)
    events_per_second: float = Field(default=20.0, ge=0)
    random_seed: int = 42
    log_every: int = Field(default=100, ge=0)
    also_unified: bool = False

    model_config = ConfigDict(frozen=True)


class ExperimentDefinition(BaseModel):
    """Configurable definition of one measurable experiment."""

    experiment_name: str = Field(min_length=1)
    description: str = ""
    settle_seconds: float = Field(default=5.0, ge=0)
    traffic: ExperimentTrafficConfig = Field(default_factory=ExperimentTrafficConfig)
    tags: list[str] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)


class ExperimentRuntimeSnapshot(BaseModel):
    """Runtime metrics captured at one point in an experiment."""

    redis_bucket_keys_scanned: int = Field(ge=0)
    redis_valid_bucket_count: int = Field(ge=0)
    redis_invalid_bucket_count: int = Field(ge=0)
    redis_total_impressions: int = Field(ge=0)
    redis_total_clicks: int = Field(ge=0)
    redis_trusted_bucket_count: int = Field(ge=0)
    redis_average_ci_width: float = Field(ge=0)
    redis_max_ci_width: float = Field(ge=0)
    current_model_snapshot_name: str | None = None
    current_model_baseline_ctr: float | None = Field(default=None, ge=0, le=1)
    postgres_model_snapshot_count: int = Field(ge=0)
    postgres_experiment_result_count: int = Field(ge=0)

    model_config = ConfigDict(frozen=True)


class ExperimentRunResult(BaseModel):
    """Structured result persisted for one experiment run."""

    experiment_name: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float = Field(ge=0)
    dry_run: bool
    config: JsonObject
    metrics: JsonObject
    artifact_uri: str | None = None
    postgres_experiment_id: int | None = None

    model_config = ConfigDict(frozen=True)
