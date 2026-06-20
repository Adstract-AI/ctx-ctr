"""Typed Kafka event models."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EventType = Literal["impression", "click"]


class CtrEvent(BaseModel):
    """Contextual CTR event sent through Kafka."""

    event_id: str
    event_type: EventType
    ad_category: str
    publisher_domain: str
    conversation_category: str
    occurred_at: datetime
    impression_event_id: str | None = None

    model_config = ConfigDict(frozen=True)

    def kafka_key(self) -> bytes:
        """Return a stable Kafka key for bucket-local ordering."""

        key = f"{self.ad_category}:{self.publisher_domain}:{self.conversation_category}"
        return key.encode("utf-8")

    def json_payload(self) -> dict[str, str | None]:
        """Return a JSON-compatible event payload."""

        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "ad_category": self.ad_category,
            "publisher_domain": self.publisher_domain,
            "conversation_category": self.conversation_category,
            "occurred_at": self.occurred_at.isoformat(),
            "impression_event_id": self.impression_event_id,
        }


class EventBatch(BaseModel):
    """Batch of CTR events produced by the simulator."""

    events: list[CtrEvent] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @property
    def impression_count(self) -> int:
        """Return the number of impression events."""

        return sum(1 for event in self.events if event.event_type == "impression")

    @property
    def click_count(self) -> int:
        """Return the number of click events."""

        return sum(1 for event in self.events if event.event_type == "click")

