"""Models for inspecting Redis runtime values."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class RedisKeyValue(BaseModel):
    """One Redis key/value payload prepared for display."""

    key: str
    value: str | None

    model_config = ConfigDict(frozen=True)


class RedisInspectSnapshot(BaseModel):
    """Redis values collected by the inspect job."""

    pattern: str
    key_count: int
    values: list[RedisKeyValue]
    bucket_key: str | None = None
    bucket_value: str | None = None

    model_config = ConfigDict(frozen=True)
