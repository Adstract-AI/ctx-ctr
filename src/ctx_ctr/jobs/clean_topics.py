"""Command entrypoint for cleaning Kafka topics."""

from __future__ import annotations

import argparse
import json

from ctx_ctr.adapters.kafka_admin import KafkaTopicAdminAdapter
from ctx_ctr.config import load_settings
from ctx_ctr.logging_config import configure_logging


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

    settings = load_settings()
    configure_logging(settings.log_level, settings.log_color)
    topic_names = args.only or [
        settings.impression_topic,
        settings.click_topic,
        settings.event_topic,
        settings.dead_letter_topic,
    ]

    if args.dry_run:
        print(json.dumps({"dry_run": True, "topics": topic_names}, indent=2, sort_keys=True))
        return

    adapter = KafkaTopicAdminAdapter(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic_names=topic_names,
    )
    adapter.clean_topics()
    print(json.dumps({"dry_run": False, "topics": topic_names}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
