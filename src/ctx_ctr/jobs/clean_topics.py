"""Command entrypoint for cleaning Kafka topics."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.kafka_admin import KafkaTopicAdminAdapter
from ctx_ctr.env_variables import KAFKA_BOOTSTRAP_SERVERS, LOG_COLOR, LOG_LEVEL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.job_configs import CleanTopicsJobConfig

logger = get_logger("ctx_ctr.jobs.clean_topics")
DEFAULT_CONFIG_PATH = "configs/clean_topics.yaml"


def main() -> None:
    """Parse CLI arguments and clean Kafka topics."""

    parser = argparse.ArgumentParser(description="Delete and recreate Kafka topics.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument(
        "--only",
        nargs="+",
        help="topic names to clean; defaults to all project topics",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="print selected topics without writing",
    )
    parser.add_argument("--impression-topic", default=None, help="default impression topic")
    parser.add_argument("--click-topic", default=None, help="default click topic")
    parser.add_argument("--event-topic", default=None, help="default unified event topic")
    parser.add_argument("--dead-letter-topic", default=None, help="default dead-letter topic")
    args = parser.parse_args()
    file_config = load_job_config(args.config, CleanTopicsJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    topic_names = config.topic_names
    logger.info(f"Selected Kafka topics: {', '.join(topic_names)}")

    if config.dry_run:
        logger.info("Running Kafka topic cleanup dry-run")
        print_success(
            "Kafka topic cleanup dry-run",
            [
                f"topics: {', '.join(topic_names)}",
                f"config: {args.config}",
                "no topics were changed",
            ],
        )
        return

    try:
        logger.info("Starting Kafka topic cleanup")
        adapter = KafkaTopicAdminAdapter(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic_names=topic_names,
        )
        adapter.clean_topics()
    except Exception as error:
        print_failure("Kafka topic cleanup", error)
        raise

    print_success(
        "Kafka topic cleanup complete",
        [f"topics: {', '.join(topic_names)}", f"config: {args.config}"],
    )


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "only",
        "dry_run",
        "impression_topic",
        "click_topic",
        "event_topic",
        "dead_letter_topic",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


if __name__ == "__main__":
    main()
