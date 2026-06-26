"""Typed models for experiment definitions and results."""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator

JsonValue: TypeAlias = Any
JsonObject: TypeAlias = dict[str, JsonValue]


class ExperimentTrafficConfig(BaseModel):
    """Traffic generation settings for one experiment run."""

    enabled: bool = True
    impressions: int | None = Field(default=None, gt=0)
    events_per_second: float | None = Field(default=None, ge=0)
    random_seed: int = 42
    log_every: int = Field(default=100, ge=0)
    also_unified: bool = False
    phases: list["ExperimentTrafficPhase"] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def validate_single_phase_fields(self) -> "ExperimentTrafficConfig":
        """Require fallback traffic fields only when no explicit phases are configured."""

        if self.phases:
            return self
        missing_fields = [
            field_name
            for field_name in (
                "impressions",
                "events_per_second",
            )
            if getattr(self, field_name) is None
        ]
        if missing_fields:
            raise ValueError(
                "traffic without phases must define "
                + ", ".join(missing_fields)
            )
        return self


class ExperimentTrafficPhase(BaseModel):
    """One traffic phase inside a full-system experiment."""

    phase_name: str = Field(min_length=1)
    impressions: int = Field(gt=0)
    events_per_second: float = Field(ge=0)
    random_seed: int
    settle_seconds: float = Field(default=5.0, ge=0)
    log_every: int = Field(default=100, ge=0)
    also_unified: bool = False

    model_config = ConfigDict(frozen=True)


class ExperimentSetupConfig(BaseModel):
    """Destructive setup actions to run before an experiment."""

    reset_values: bool = False
    clean_topics: bool = False
    seed_values: bool = False

    model_config = ConfigDict(frozen=True)


class ExperimentProcessorConfig(BaseModel):
    """Runtime settings for one processor started by an experiment."""

    enabled: bool = False
    command: list[str] = Field(default_factory=list)
    startup_seconds: float = Field(default=8.0, ge=0)
    stop_timeout_seconds: float = Field(default=15.0, gt=0)

    model_config = ConfigDict(frozen=True)


class ExperimentProcessorsConfig(BaseModel):
    """Processor jobs controlled by a full-system experiment."""

    realtime_ctr: ExperimentProcessorConfig = Field(default_factory=ExperimentProcessorConfig)
    streaming_weight_update: ExperimentProcessorConfig = Field(
        default_factory=ExperimentProcessorConfig
    )

    model_config = ConfigDict(frozen=True)


class ExperimentSuccessGates(BaseModel):
    """Strict acceptance checks for full-system experiment runs."""

    require_processor_health: bool = False
    require_redis_impression_delta_match: bool = False
    require_redis_click_delta_match: bool = False
    require_model_snapshot_created: bool = False
    max_invalid_redis_buckets: int = Field(default=0, ge=0)

    model_config = ConfigDict(frozen=True)


class ExperimentDefinition(BaseModel):
    """Configurable definition of one measurable experiment."""

    experiment_name: str = Field(min_length=1)
    description: str = ""
    settle_seconds: float = Field(default=5.0, ge=0)
    setup: ExperimentSetupConfig = Field(default_factory=ExperimentSetupConfig)
    processors: ExperimentProcessorsConfig = Field(default_factory=ExperimentProcessorsConfig)
    traffic: ExperimentTrafficConfig = Field(default_factory=ExperimentTrafficConfig)
    success_gates: ExperimentSuccessGates = Field(default_factory=ExperimentSuccessGates)
    tags: list[str] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def validate_processor_commands(self) -> "ExperimentDefinition":
        """Ensure enabled processors have explicit commands."""

        enabled_processors = (
            self.processors.realtime_ctr,
            self.processors.streaming_weight_update,
        )
        if any(processor.enabled and not processor.command for processor in enabled_processors):
            raise ValueError("enabled experiment processors must define a command")
        return self


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
