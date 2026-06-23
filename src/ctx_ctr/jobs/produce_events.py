"""Command entrypoint for producing simulated CTR events."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.kafka_event_producer import KafkaEventProducerAdapter
from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.job_configs import ProduceEventsJobConfig
from ctx_ctr.services.event_simulator import EventProducerRunConfig, EventSimulatorService

logger = get_logger("ctx_ctr.jobs.produce_events")
DEFAULT_CONFIG_PATH = "configs/produce_events.yaml"


class DryRunEventPublisher:
    """No-op event publisher used for offline dry-run validation."""

    def publish(self, event: CtrEvent, *, also_unified: bool) -> None:
        """Skip event publishing."""

    def flush(self) -> None:
        """Skip producer flush."""


def main() -> None:
    """Parse CLI arguments and produce simulated CTR events."""

    parser = argparse.ArgumentParser(description="Produce simulated CTR events to Kafka.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--impressions", type=int, default=None, help="number of impressions to simulate")
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=None,
        help="send rate; use 0 to publish as fast as possible",
    )
    parser.add_argument("--random-seed", type=int, default=None, help="deterministic random seed")
    parser.add_argument(
        "--log-every",
        type=int,
        default=None,
        help="log progress every N produced events; use 0 to disable progress logs",
    )
    parser.add_argument(
        "--also-unified",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="also publish events to ctr.events",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="simulate without publishing to Kafka",
    )
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--impression-topic", default=None, help="impression output topic")
    parser.add_argument("--click-topic", default=None, help="click output topic")
    parser.add_argument("--event-topic", default=None, help="unified event output topic")
    args = parser.parse_args()
    file_config = load_job_config(args.config, ProduceEventsJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    run_config = EventProducerRunConfig(
        impressions=config.impressions,
        events_per_second=config.events_per_second,
        random_seed=config.random_seed,
        log_every=config.log_every,
        also_unified=config.also_unified,
        dry_run=config.dry_run,
    )

    logger.info(
        f"Configured event producer: impressions={run_config.impressions}, "
        f"events_per_second={run_config.events_per_second}, random_seed={run_config.random_seed}, "
        f"also_unified={run_config.also_unified}, dry_run={run_config.dry_run}, "
        f"config={args.config}"
    )

    if config.dry_run:
        service = EventSimulatorService(DryRunEventPublisher())
        result = service.produce(run_config)
        print_success(
            "Event production dry-run",
            [
                f"impressions: {result.impressions}",
                f"clicks: {result.clicks}",
                f"total events: {result.total_events}",
                f"config: {args.config}",
            ],
        )
        return

    producer: KafkaEventProducerAdapter | None = None
    try:
        producer = KafkaEventProducerAdapter(
            bootstrap_servers=config.bootstrap_servers,
            impression_topic=config.impression_topic,
            click_topic=config.click_topic,
            event_topic=config.event_topic,
        )
        service = EventSimulatorService(producer)
        result = service.produce(run_config)
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
            f"config: {args.config}",
        ],
    )


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "impressions",
        "events_per_second",
        "random_seed",
        "log_every",
        "also_unified",
        "dry_run",
        "bootstrap_servers",
        "impression_topic",
        "click_topic",
        "event_topic",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


if __name__ == "__main__":
    main()
