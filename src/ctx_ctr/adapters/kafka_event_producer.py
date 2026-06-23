"""Kafka producer adapter for CTR events."""

from __future__ import annotations

import json

from kafka import KafkaProducer  # type: ignore[import-untyped]
from kafka.errors import KafkaError  # type: ignore[import-untyped]

from ctx_ctr.exceptions import CtxCtrError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.events import CtrEvent

logger = get_logger(__name__)


class EventProducerError(CtxCtrError):
    """Raised when CTR events cannot be produced to Kafka."""


class KafkaEventProducerAdapter:
    """Publish CTR events to Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str,
        impression_topic: str,
        click_topic: str,
        event_topic: str,
    ) -> None:
        self._impression_topic = impression_topic
        self._click_topic = click_topic
        self._event_topic = event_topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    def publish(self, event: CtrEvent, *, also_unified: bool) -> None:
        """Publish one event to its type-specific topic and optionally the unified topic."""

        topic = self._topic_for_event(event)
        try:
            self._producer.send(topic, key=event.kafka_key(), value=event.json_payload())
            if also_unified:
                self._producer.send(
                    self._event_topic,
                    key=event.kafka_key(),
                    value=event.json_payload(),
                )
        except KafkaError as error:
            raise EventProducerError("Failed to publish CTR event") from error

    def flush(self) -> None:
        """Flush pending Kafka records."""

        try:
            self._producer.flush()
        except KafkaError as error:
            raise EventProducerError("Failed to flush CTR events") from error

    def close(self) -> None:
        """Close the Kafka producer."""

        self._producer.close()

    def _topic_for_event(self, event: CtrEvent) -> str:
        if event.event_type == "impression":
            return self._impression_topic
        return self._click_topic

