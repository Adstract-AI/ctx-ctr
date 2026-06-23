"""Command entrypoint for cleaning Kafka topics."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.kafka_admin import KafkaTopicAdminAdapter
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
    DEAD_LETTER_TOPIC,
    EVENT_TOPIC,
    IMPRESSION_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    LOG_COLOR,
    LOG_LEVEL,
)
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger

logger = get_logger("ctx_ctr.jobs.clean_topics")


def main() -> None:
    """Parse CLI arguments and clean Kafka topics."""

    parser = argparse.ArgumentParser(description="Delete and recreate Kafka topics.")
    parser.add_argument(
        "--only",
        nargs="+",
        help="topic names to clean; defaults to all project topics",
    )
    parser.add_argument("--dry-run", action="store_true", help="print selected topics without writing")
    args = parser.parse_args()

    configure_logging(LOG_LEVEL, LOG_COLOR)
    topic_names = args.only or [
        IMPRESSION_TOPIC,
        CLICK_TOPIC,
        EVENT_TOPIC,
        DEAD_LETTER_TOPIC,
    ]
    logger.info(f"Selected Kafka topics: {', '.join(topic_names)}")

    if args.dry_run:
        logger.info("Running Kafka topic cleanup dry-run")
        print_success(
            "Kafka topic cleanup dry-run",
            [
                f"topics: {', '.join(topic_names)}",
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
        [f"topics: {', '.join(topic_names)}"],
    )


if __name__ == "__main__":
    main()
