"""Command entrypoint for Task 2 periodic weight recalibration."""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext

from ctx_ctr.config import load_settings
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.weight_update import WeightUpdateResult, WeightUpdateRunConfig
from ctx_ctr.services.weight_update_service import (
    WeightUpdatePostgresWriter,
    WeightUpdateRedisStore,
    WeightUpdateService,
)

logger = get_logger("ctx_ctr.jobs.run_weight_update")

DEFAULT_INTERVAL_SECONDS = 3600
DEFAULT_LEARNING_RATE = 0.25
DEFAULT_EVIDENCE_SMOOTHING = 1000.0
DEFAULT_RIDGE = 0.01
DEFAULT_MAX_DELTA = 0.25
DEFAULT_MIN_TRUSTED_BUCKETS = 1
DEFAULT_SNAPSHOT_NAME_PREFIX = "flink_weight_update"


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and run the Task 2 periodic weight-update job."""

    settings = load_settings()
    parser = _build_parser(settings.redis_url, settings.postgres_dsn)
    args = parser.parse_args(argv)
    configure_logging(settings.log_level, settings.log_color)

    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be greater than zero")

    run_config = WeightUpdateRunConfig(
        learning_rate=args.learning_rate,
        evidence_smoothing=args.evidence_smoothing,
        ridge=args.ridge,
        max_delta=args.max_delta,
        min_trusted_buckets=args.min_trusted_buckets,
        snapshot_name_prefix=args.snapshot_name_prefix,
        dry_run=args.dry_run,
    )
    logger.info(
        f"Configured weight update job: redis_url={args.redis_url}, "
        f"postgres_dsn={args.postgres_dsn}, once={args.once}, dry_run={args.dry_run}, "
        f"interval_seconds={args.interval_seconds}, learning_rate={args.learning_rate}, "
        f"evidence_smoothing={args.evidence_smoothing}, ridge={args.ridge}, "
        f"max_delta={args.max_delta}, min_trusted_buckets={args.min_trusted_buckets}, "
        f"snapshot_name_prefix={args.snapshot_name_prefix}"
    )

    try:
        redis_store = _build_redis_store(args.redis_url)
        with _open_postgres_writer(args.postgres_dsn, args.dry_run) as postgres_writer:
            service = WeightUpdateService(redis_store=redis_store, postgres_writer=postgres_writer)
            if args.once:
                result = service.recalibrate(run_config)
                _log_result(result)
                print_success(_result_title(result), _result_lines(result))
                return

            while True:
                result = service.recalibrate(run_config)
                _log_result(result)
                logger.info(
                    f"Sleeping {args.interval_seconds} seconds before the next weight update run"
                )
                time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        logger.info("Weight update job stopped")
    except Exception as error:
        print_failure("Weight update", error)
        raise


def _build_parser(default_redis_url: str, default_postgres_dsn: str) -> argparse.ArgumentParser:
    """Build the CLI parser for the Task 2 weight-update job."""

    parser = argparse.ArgumentParser(description="Run the Task 2 weight update job.")
    parser.add_argument("--redis-url", default=default_redis_url, help="Redis connection URL")
    parser.add_argument(
        "--postgres-dsn",
        default=default_postgres_dsn,
        help="PostgreSQL DSN used for model snapshot history",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="seconds to wait between periodic recalibration runs",
    )
    parser.add_argument("--once", action="store_true", help="run one recalibration and exit")
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=DEFAULT_LEARNING_RATE,
        help="base learning rate for feature-family updates",
    )
    parser.add_argument(
        "--evidence-smoothing",
        type=float,
        default=DEFAULT_EVIDENCE_SMOOTHING,
        help="evidence smoothing term for the learning rate",
    )
    parser.add_argument(
        "--ridge",
        type=float,
        default=DEFAULT_RIDGE,
        help="ridge regularization applied to each feature weight",
    )
    parser.add_argument(
        "--max-delta",
        type=float,
        default=DEFAULT_MAX_DELTA,
        help="maximum absolute per-feature update before centering",
    )
    parser.add_argument(
        "--min-trusted-buckets",
        type=int,
        default=DEFAULT_MIN_TRUSTED_BUCKETS,
        help="minimum trusted buckets required before a run can write outputs",
    )
    parser.add_argument(
        "--snapshot-name-prefix",
        default=DEFAULT_SNAPSHOT_NAME_PREFIX,
        help="prefix used when generating snapshot names",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="compute Task 2 updates without writing Redis or PostgreSQL",
    )
    return parser


def _build_redis_store(redis_url: str) -> WeightUpdateRedisStore:
    """Construct the Redis runtime adapter lazily for easier local dry-run testing."""

    from ctx_ctr.adapters.redis_runtime import RedisRuntimeAdapter

    return RedisRuntimeAdapter(redis_url)


def _open_postgres_writer(
    postgres_dsn: str,
    dry_run: bool,
) -> AbstractContextManager[WeightUpdatePostgresWriter | None]:
    """Open the PostgreSQL runtime adapter only when the run is allowed to write."""

    if dry_run:
        return nullcontext(None)

    from ctx_ctr.adapters.postgres_runtime import PostgresRuntimeAdapter

    return PostgresRuntimeAdapter(postgres_dsn)


def _result_title(result: WeightUpdateResult) -> str:
    """Return the once-run summary title for the current result."""

    if not result.accepted:
        return "Weight update skipped"
    if result.dry_run:
        return "Weight update dry-run"
    return "Weight update complete"


def _result_lines(result: WeightUpdateResult) -> list[str]:
    """Return human-readable summary lines for one run result."""

    return [
        f"redis bucket keys scanned: {result.metrics.input_bucket_count}",
        f"valid buckets parsed: {result.metrics.valid_bucket_count}",
        f"invalid buckets parsed: {result.metrics.invalid_bucket_count}",
        f"trusted buckets used: {result.metrics.trusted_bucket_count}",
        f"skipped/untrusted buckets: {result.metrics.skipped_bucket_count}",
        f"unknown feature values initialized: {result.metrics.unknown_feature_count}",
        f"w0 unchanged: {result.metrics.w0_unchanged}",
        f"max absolute weight delta: {result.metrics.max_absolute_weight_delta:.6f}",
        f"redis write status: {result.redis_write_applied}",
        f"postgres snapshot status: {result.postgres_write_applied}",
        f"snapshot name: {result.snapshot_name or 'not generated'}",
        f"snapshot id: {result.postgres_snapshot_id if result.postgres_snapshot_id is not None else 'n/a'}",
        f"skipped reason: {result.skipped_reason or 'none'}",
    ]


def _log_result(result: WeightUpdateResult) -> None:
    """Log the required Task 2 run summary after each recalibration pass."""

    logger.info(
        f"Weight update summary: scanned={result.metrics.input_bucket_count}, "
        f"valid={result.metrics.valid_bucket_count}, invalid={result.metrics.invalid_bucket_count}, "
        f"trusted={result.metrics.trusted_bucket_count}, skipped={result.metrics.skipped_bucket_count}, "
        f"unknown_features={result.metrics.unknown_feature_count}, "
        f"w0_unchanged={result.metrics.w0_unchanged}, "
        f"max_delta={result.metrics.max_absolute_weight_delta:.6f}, "
        f"redis_write={result.redis_write_applied}, "
        f"postgres_write={result.postgres_write_applied}, "
        f"snapshot_name={result.snapshot_name or 'not_generated'}, "
        f"snapshot_id={result.postgres_snapshot_id if result.postgres_snapshot_id is not None else 'n/a'}"
    )


if __name__ == "__main__":
    main()
