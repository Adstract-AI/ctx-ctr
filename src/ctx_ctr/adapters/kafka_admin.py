"""Kafka admin adapter for explicit topic cleanup."""

from __future__ import annotations

import time
from collections.abc import Mapping

from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore[import-untyped]
from kafka.errors import KafkaError, UnknownTopicOrPartitionError  # type: ignore[import-untyped]

from ctx_ctr.exceptions import SeedError
from ctx_ctr.constants import DEFAULT_KAFKA_TOPIC_PARTITIONS
from ctx_ctr.logging_config import get_logger

logger = get_logger(__name__)


class KafkaTopicAdminAdapter:
    """Delete and recreate selected Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic_names: list[str],
        *,
        partition_counts: Mapping[str, int] | None = None,
        default_partition_count: int = DEFAULT_KAFKA_TOPIC_PARTITIONS,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic_names = topic_names
        self._partition_counts = dict(partition_counts or {})
        self._default_partition_count = default_partition_count

    def clean_topics(self) -> None:
        """Delete and recreate selected topics."""

        client = KafkaAdminClient(bootstrap_servers=self._bootstrap_servers, client_id="ctx-ctr-topic-cleaner")
        try:
            try:
                client.delete_topics(self._topic_names)
                time.sleep(2)
            except UnknownTopicOrPartitionError:
                logger.debug("Kafka topics did not exist during reset")
            topics = [
                NewTopic(
                    name=name,
                    num_partitions=self._partition_counts.get(
                        name,
                        self._default_partition_count,
                    ),
                    replication_factor=1,
                )
                for name in self._topic_names
            ]
            client.create_topics(topics, validate_only=False)
            logger.debug(f"Cleaned Kafka topics: {', '.join(self._topic_names)}")
        except KafkaError as error:
            raise SeedError("Failed to clean Kafka topics") from error
        finally:
            client.close()
