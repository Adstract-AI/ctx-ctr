"""Command entrypoint for producing simulated CTR events."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.kafka_event_producer import KafkaEventProducerAdapter
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
    EVENT_TOPIC,
    IMPRESSION_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    LOG_COLOR,
    LOG_LEVEL,
)
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.services.event_simulator import EventProducerRunConfig, EventSimulatorService

logger = get_logger("ctx_ctr.jobs.produce_events")


class DryRunEventPublisher:
    """No-op event publisher used for offline dry-run validation."""

    def publish(self, event: CtrEvent, *, also_unified: bool) -> None:
        """Skip event publishing."""

    def flush(self) -> None:
        """Skip producer flush."""


def main() -> None:
    """Parse CLI arguments and produce simulated CTR events."""

    parser = argparse.ArgumentParser(description="Produce simulated CTR events to Kafka.")
    parser.add_argument("--impressions", type=int, default=100, help="number of impressions to simulate")
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=20.0,
        help="send rate; use 0 to publish as fast as possible",
    )
    parser.add_argument("--random-seed", type=int, default=42, help="deterministic random seed")
    parser.add_argument(
        "--log-every",
        type=int,
        default=10,
        help="log progress every N produced events; use 0 to disable progress logs",
    )
    parser.add_argument("--also-unified", action="store_true", help="also publish events to ctr.events")
    parser.add_argument("--dry-run", action="store_true", help="simulate without publishing to Kafka")
    args = parser.parse_args()

    configure_logging(LOG_LEVEL, LOG_COLOR)
    config = EventProducerRunConfig(
        impressions=args.impressions,
        events_per_second=args.events_per_second,
        random_seed=args.random_seed,
        log_every=args.log_every,
        also_unified=args.also_unified,
        dry_run=args.dry_run,
    )

    logger.info(
        f"Configured event producer: impressions={config.impressions}, "
        f"events_per_second={config.events_per_second}, random_seed={config.random_seed}, "
        f"also_unified={config.also_unified}, dry_run={config.dry_run}"
    )

    if args.dry_run:
        service = EventSimulatorService(DryRunEventPublisher())
        result = service.produce(config)
        print_success(
            "Event production dry-run",
            [
                f"impressions: {result.impressions}",
                f"clicks: {result.clicks}",
                f"total events: {result.total_events}",
            ],
        )
        return

    producer: KafkaEventProducerAdapter | None = None
    try:
        producer = KafkaEventProducerAdapter(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            impression_topic=IMPRESSION_TOPIC,
            click_topic=CLICK_TOPIC,
            event_topic=EVENT_TOPIC,
        )
        service = EventSimulatorService(producer)
        result = service.produce(config)
    except Exception as error:
        print_failure("Event production", error)
        raise
    finally:
        if producer is not None:
            producer.close()

    print_success(
        "Event production complete",
        [
            f"impressions: {result.impressions}",
            f"clicks: {result.clicks}",
            f"total events: {result.total_events}",
        ],
    )


if __name__ == "__main__":
    main()
