"""Command entrypoint for producing simulated CTR events."""

from __future__ import annotations

import argparse
import json

from ctx_ctr.adapters.kafka_event_producer import KafkaEventProducerAdapter
from ctx_ctr.config import load_settings
from ctx_ctr.logging_config import configure_logging
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.services.event_simulator import EventProducerRunConfig, EventSimulatorService


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
    parser.add_argument("--also-unified", action="store_true", help="also publish events to ctr.events")
    parser.add_argument("--dry-run", action="store_true", help="simulate without publishing to Kafka")
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level, settings.log_color)
    config = EventProducerRunConfig(
        impressions=args.impressions,
        events_per_second=args.events_per_second,
        random_seed=args.random_seed,
        also_unified=args.also_unified,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        service = EventSimulatorService(DryRunEventPublisher())
        result = service.produce(config)
        print(json.dumps(result.model_dump(), indent=2, sort_keys=True))
        return

    producer = KafkaEventProducerAdapter(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        impression_topic=settings.impression_topic,
        click_topic=settings.click_topic,
        event_topic=settings.event_topic,
    )
    try:
        service = EventSimulatorService(producer)
        result = service.produce(config)
    finally:
        producer.close()

    print(json.dumps(result.model_dump(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

