"""Typed models for Redis-to-Postgres bucket persistence."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class PersistRedisBucketsResult(BaseModel):
    """Outcome of one Redis bucket persistence run."""

    dry_run: bool
    scanned_key_count: int = Field(ge=0)
    valid_bucket_count: int = Field(ge=0)
    invalid_bucket_count: int = Field(ge=0)
    persisted_bucket_count: int = Field(ge=0)
    postgres_write_applied: bool

    model_config = ConfigDict(frozen=True)
