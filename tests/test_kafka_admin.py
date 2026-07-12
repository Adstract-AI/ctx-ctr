from __future__ import annotations

from typing import Any

from ctx_ctr.adapters import kafka_admin
from ctx_ctr.adapters.kafka_admin import KafkaTopicAdminAdapter


class FakeKafkaAdminClient:
    def __init__(self) -> None:
        self.created_topics: list[Any] = []
        self.deleted_topics: list[str] = []
        self.closed = False

    def delete_topics(self, topic_names: list[str]) -> None:
        self.deleted_topics = topic_names

    def create_topics(self, topics: list[Any], *, validate_only: bool) -> None:
        assert validate_only is False
        self.created_topics = topics

    def close(self) -> None:
        self.closed = True


def test_clean_topics_recreates_configured_partition_counts(monkeypatch: Any) -> None:
    client = FakeKafkaAdminClient()
    monkeypatch.setattr(kafka_admin, "KafkaAdminClient", lambda **_kwargs: client)
    monkeypatch.setattr(kafka_admin.time, "sleep", lambda _seconds: None)
    topics = ["ctr.impressions", "ctr.clicks", "ctr.events", "ctr.dead-letter"]

    KafkaTopicAdminAdapter(
        bootstrap_servers="localhost:9092",
        topic_names=topics,
        partition_counts={
            "ctr.impressions": 6,
            "ctr.clicks": 6,
            "ctr.events": 6,
            "ctr.dead-letter": 3,
        },
    ).clean_topics()

    assert client.deleted_topics == topics
    assert {
        topic.name: topic.num_partitions for topic in client.created_topics
    } == {
        "ctr.impressions": 6,
        "ctr.clicks": 6,
        "ctr.events": 6,
        "ctr.dead-letter": 3,
    }
    assert client.closed is True
