"""Command entrypoint for printing Redis runtime values."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from ctx_ctr.adapters.redis_inspect import RedisInspectAdapter
from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.ctr_state import CtrBucketKey
from ctx_ctr.models.job_configs import WatchRedisValuesJobConfig
from ctx_ctr.models.redis_inspect import RedisInspectSnapshot
from ctx_ctr.services.redis_inspect import RedisInspectService

logger = get_logger("ctx_ctr.jobs.watch_redis_values")
DEFAULT_CONFIG_PATH = "configs/watch_redis_values.yaml"


def main() -> None:
    """Parse CLI arguments and print Redis values."""

    parser = argparse.ArgumentParser(description="Print Redis values for local CTR debugging.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--redis-url", default=None, help="Redis URL")
    parser.add_argument("--pattern", default=None, help="Redis scan pattern")
    parser.add_argument("--limit", type=int, default=None, help="maximum number of keys to print")
    parser.add_argument(
        "--watch",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="keep printing snapshots until interrupted",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="seconds between snapshots when --watch is enabled",
    )
    parser.add_argument(
        "--pretty-json",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="pretty-print JSON Redis values",
    )
    parser.add_argument(
        "--only-bucket",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="print only the configured focused bucket",
    )
    parser.add_argument(
        "--bucket",
        nargs=3,
        metavar=("AD_CATEGORY", "PUBLISHER_DOMAIN", "CONVERSATION_CATEGORY"),
        help="focused CTR bucket to print separately",
    )
    args = parser.parse_args()
    file_config = load_job_config(args.config, WatchRedisValuesJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    logger.info(
        "Configured Redis value watcher: "
        f"pattern={config.pattern}, limit={config.limit}, watch={config.watch}, "
        f"interval_seconds={config.interval_seconds}, config={args.config}"
    )

    try:
        _run(config)
    except KeyboardInterrupt:
        print_success("Redis value watch stopped", ["status: interrupted by user"])
    except Exception as error:
        print_failure("Redis value watch", error)
        raise


def _run(config: WatchRedisValuesJobConfig) -> None:
    adapter = RedisInspectAdapter(config.redis_url)
    service = RedisInspectService(adapter)
    bucket_key = _bucket_from_config(config)
    snapshots_printed = 0

    try:
        while True:
            snapshot = service.collect_snapshot(
                pattern=config.pattern,
                limit=config.limit,
                bucket_key=bucket_key,
                only_bucket=config.only_bucket,
            )
            snapshots_printed += 1
            _print_snapshot(snapshot, pretty_json=config.pretty_json)

            if not config.watch:
                print_success(
                    "Redis values printed",
                    [
                        f"pattern: {config.pattern}",
                        f"keys printed: {snapshot.key_count}",
                        f"focused bucket: {snapshot.bucket_key or 'none'}",
                    ],
                )
                return

            logger.info(f"Printed Redis snapshot {snapshots_printed}")
            time.sleep(config.interval_seconds)
    finally:
        adapter.close()


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "redis_url",
        "pattern",
        "limit",
        "watch",
        "interval_seconds",
        "pretty_json",
        "only_bucket",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value

    if args.bucket is not None:
        ad_category, publisher_domain, conversation_category = args.bucket
        overrides["ad_category"] = ad_category
        overrides["publisher_domain"] = publisher_domain
        overrides["conversation_category"] = conversation_category

    return overrides


def _bucket_from_config(config: WatchRedisValuesJobConfig) -> CtrBucketKey | None:
    bucket_values = [
        config.ad_category,
        config.publisher_domain,
        config.conversation_category,
    ]
    if all(value is None for value in bucket_values):
        return None
    if any(value is None for value in bucket_values):
        raise ValueError(
            "Focused bucket requires ad_category, publisher_domain, and conversation_category"
        )
    return CtrBucketKey(
        ad_category=str(config.ad_category),
        publisher_domain=str(config.publisher_domain),
        conversation_category=str(config.conversation_category),
    )


def _print_snapshot(snapshot: RedisInspectSnapshot, *, pretty_json: bool) -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"\n=== Redis snapshot at {timestamp} ===")
    print(f"pattern: {snapshot.pattern}")
    print(f"keys printed: {snapshot.key_count}")

    for item in snapshot.values:
        print(f"\n[{item.key}]")
        print(_format_value(item.value, pretty_json=pretty_json))

    if snapshot.bucket_key is not None:
        print(f"\n[focused bucket: {snapshot.bucket_key}]")
        print(_format_value(snapshot.bucket_value, pretty_json=pretty_json))


def _format_value(value: str | None, *, pretty_json: bool) -> str:
    if value is None:
        return "<missing>"
    if not pretty_json:
        return value

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value

    return json.dumps(parsed, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
