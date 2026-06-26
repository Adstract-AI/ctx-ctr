"""Typed models for Flink-native streaming weight updates."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot


class StreamingTripletCounter(BaseModel):
    """Cumulative impression/click counts for one contextual triplet."""

    ad_category: str
    publisher_domain: str
    conversation_category: str
    impressions: int = Field(default=0, ge=0)
    clicks: int = Field(default=0, ge=0)

    model_config = ConfigDict(frozen=True)

    @property
    def key(self) -> str:
        """Return the compact triplet key used in Flink state."""

        return f"{self.ad_category}:{self.publisher_domain}:{self.conversation_category}"


class StreamingWeightEvidenceState(BaseModel):
    """Cumulative state kept by the streaming weight-update operator."""

    triplets: dict[str, StreamingTripletCounter] = Field(default_factory=dict)
    processed_events: int = Field(default=0, ge=0)
    invalid_events: int = Field(default=0, ge=0)
    next_timer_ms: int | None = None

    model_config = ConfigDict(frozen=True)


class StreamingWeightEvidenceSnapshot(BaseModel):
    """Bucket evidence produced from streaming counters."""

    scanned_key_count: int = Field(ge=0)
    valid_bucket_count: int = Field(ge=0)
    invalid_bucket_count: int = Field(ge=0)
    buckets: list[SeedBucketStatistic] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)


class StreamingWeightUpdateTimerResult(BaseModel):
    """Summary emitted after one timer-based recalibration attempt."""

    processed_events: int = Field(ge=0)
    invalid_events: int = Field(ge=0)
    triplet_count: int = Field(ge=0)
    current_model: SeedModelSnapshot
    summary: str

    model_config = ConfigDict(frozen=True)
