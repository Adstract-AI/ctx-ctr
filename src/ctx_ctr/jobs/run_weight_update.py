"""Command entrypoint for Task 2 periodic weight recalibration."""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext

from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.job_configs import RunWeightUpdateJobConfig
from ctx_ctr.models.weight_update import WeightUpdateResult, WeightUpdateRunConfig
from ctx_ctr.services.weight_update_service import (
    WeightUpdatePostgresWriter,
    WeightUpdateRedisStore,
    WeightUpdateService,
)

logger = get_logger("ctx_ctr.jobs.run_weight_update")
DEFAULT_CONFIG_PATH = "configs/run_weight_update.yaml"


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and run the Task 2 periodic weight-update job."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    file_config = load_job_config(args.config, RunWeightUpdateJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))
    configure_logging(LOG_LEVEL, LOG_COLOR)

    run_config = WeightUpdateRunConfig(
        learning_rate=config.learning_rate,
        evidence_smoothing=config.evidence_smoothing,
        ridge=config.ridge,
        max_delta=config.max_delta,
        min_trusted_buckets=config.min_trusted_buckets,
        snapshot_name_prefix=config.snapshot_name_prefix,
        baseline_update=config.baseline_update,
        baseline_learning_rate=config.baseline_learning_rate,
        baseline_evidence_smoothing=config.baseline_evidence_smoothing,
        baseline_max_delta=config.baseline_max_delta,
        baseline_min_impressions=config.baseline_min_impressions,
        baseline_max_ci_width=config.baseline_max_ci_width,
        dry_run=config.dry_run,
    )
    logger.info(
        f"Configured weight update job: redis_url={config.redis_url}, "
        f"postgres_dsn={config.postgres_dsn}, once={config.once}, dry_run={config.dry_run}, "
        f"interval_seconds={config.interval_seconds}, learning_rate={config.learning_rate}, "
        f"evidence_smoothing={config.evidence_smoothing}, ridge={config.ridge}, "
        f"max_delta={config.max_delta}, min_trusted_buckets={config.min_trusted_buckets}, "
        f"baseline_update={config.baseline_update}, "
        f"baseline_learning_rate={config.baseline_learning_rate}, "
        f"baseline_evidence_smoothing={config.baseline_evidence_smoothing}, "
        f"baseline_max_delta={config.baseline_max_delta}, "
        f"baseline_min_impressions={config.baseline_min_impressions}, "
        f"baseline_max_ci_width={config.baseline_max_ci_width}, "
        f"snapshot_name_prefix={config.snapshot_name_prefix}, config={args.config}"
    )

    try:
        redis_store = _build_redis_store(config.redis_url)
        with _open_postgres_writer(config.postgres_dsn, config.dry_run) as postgres_writer:
            service = WeightUpdateService(redis_store=redis_store, postgres_writer=postgres_writer)
            last_result: WeightUpdateResult | None = None
            if config.once:
                result = service.recalibrate(run_config)
                last_result = result
                _log_result(result)
                _print_result_summary(result, config_path=args.config)
                return

            while True:
                result = service.recalibrate(run_config)
                last_result = result
                _log_result(result)
                _print_result_summary(
                    result,
                    config_path=args.config,
                    extra_lines=[f"next run in seconds: {config.interval_seconds}"],
                )
                logger.info(
                    f"Sleeping {config.interval_seconds} seconds before the next weight update run"
                )
                time.sleep(config.interval_seconds)
    except KeyboardInterrupt:
        lines = ["status: interrupted by user"]
        if "last_result" in locals() and last_result is not None:
            lines.extend(_result_lines(last_result))
        print_success("Weight update job stopped", lines)
    except Exception as error:
        print_failure("Weight update", error)
        raise


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the Task 2 weight-update job."""

    parser = argparse.ArgumentParser(description="Run the Task 2 weight update job.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--redis-url", default=None, help="Redis connection URL")
    parser.add_argument(
        "--postgres-dsn",
        default=None,
        help="PostgreSQL DSN used for model snapshot history",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="seconds to wait between periodic recalibration runs",
    )
    parser.add_argument(
        "--once",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="run one recalibration and exit",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=None,
        help="base learning rate for feature-family updates",
    )
    parser.add_argument(
        "--evidence-smoothing",
        type=float,
        default=None,
        help="evidence smoothing term for the learning rate",
    )
    parser.add_argument(
        "--ridge",
        type=float,
        default=None,
        help="ridge regularization applied to each feature weight",
    )
    parser.add_argument(
        "--max-delta",
        type=float,
        default=None,
        help="maximum absolute per-feature update before centering",
    )
    parser.add_argument(
        "--min-trusted-buckets",
        type=int,
        default=None,
        help="minimum trusted buckets required before a run can write outputs",
    )
    parser.add_argument(
        "--snapshot-name-prefix",
        default=None,
        help="prefix used when generating snapshot names",
    )
    parser.add_argument(
        "--baseline-update",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="enable or disable global baseline w0 updates",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        default=None,
        dest="baseline_update",
        help="alias for --baseline-update",
    )
    parser.add_argument(
        "--disable-baseline-update",
        action="store_false",
        default=None,
        dest="baseline_update",
        help="alias for --no-baseline-update",
    )
    parser.add_argument(
        "--baseline-learning-rate",
        type=float,
        default=None,
        help="base learning rate for global baseline updates",
    )
    parser.add_argument(
        "--baseline-evidence-smoothing",
        type=float,
        default=None,
        help="evidence smoothing term for the baseline learning rate",
    )
    parser.add_argument(
        "--baseline-max-delta",
        type=float,
        default=None,
        help="maximum absolute global baseline update",
    )
    parser.add_argument(
        "--baseline-min-impressions",
        type=int,
        default=None,
        help="minimum global impressions required before updating the baseline",
    )
    parser.add_argument(
        "--baseline-max-ci-width",
        type=float,
        default=None,
        help="maximum posterior confidence interval width allowed for baseline updates",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="compute Task 2 updates without writing Redis or PostgreSQL",
    )
    return parser


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "redis_url",
        "postgres_dsn",
        "interval_seconds",
        "once",
        "learning_rate",
        "evidence_smoothing",
        "ridge",
        "max_delta",
        "min_trusted_buckets",
        "snapshot_name_prefix",
        "baseline_update",
        "baseline_learning_rate",
        "baseline_evidence_smoothing",
        "baseline_max_delta",
        "baseline_min_impressions",
        "baseline_max_ci_width",
        "dry_run",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


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
        f"baseline update enabled: {result.metrics.baseline_update_enabled}",
        f"baseline update applied: {result.metrics.baseline_update_applied}",
        f"old w0: {result.metrics.old_w0:.6f}",
        f"new w0: {result.metrics.new_w0:.6f}",
        f"baseline delta: {result.metrics.baseline_delta:.6f}",
        f"aggregate impressions/clicks: "
        f"{result.metrics.aggregate_impressions}/{result.metrics.aggregate_clicks}",
        f"baseline ci width: {result.metrics.baseline_ci_width:.6f}",
        f"baseline guard reason: {result.metrics.baseline_guard_reason}",
        f"max absolute weight delta: {result.metrics.max_absolute_weight_delta:.6f}",
        f"redis write status: {result.redis_write_applied}",
        f"postgres snapshot status: {result.postgres_write_applied}",
        f"snapshot name: {result.snapshot_name or 'not generated'}",
        "snapshot id: "
        f"{result.postgres_snapshot_id if result.postgres_snapshot_id is not None else 'n/a'}",
        f"skipped reason: {result.skipped_reason or 'none'}",
    ]


def _print_result_summary(
    result: WeightUpdateResult,
    *,
    config_path: str,
    extra_lines: list[str] | None = None,
) -> None:
    """Print a boxed summary for one weight-update recalibration pass."""

    print_success(
        _result_title(result),
        [
            *_result_lines(result),
            *(extra_lines or []),
            f"config: {config_path}",
        ],
    )


def _log_result(result: WeightUpdateResult) -> None:
    """Log the required Task 2 run summary after each recalibration pass."""

    logger.info(
        f"Weight update summary: scanned={result.metrics.input_bucket_count}, "
        f"valid={result.metrics.valid_bucket_count}, "
        f"invalid={result.metrics.invalid_bucket_count}, "
        f"trusted={result.metrics.trusted_bucket_count}, "
        f"skipped={result.metrics.skipped_bucket_count}, "
        f"unknown_features={result.metrics.unknown_feature_count}, "
        f"w0_unchanged={result.metrics.w0_unchanged}, "
        f"baseline_update={result.metrics.baseline_update_enabled}, "
        f"baseline_applied={result.metrics.baseline_update_applied}, "
        f"old_w0={result.metrics.old_w0:.6f}, "
        f"new_w0={result.metrics.new_w0:.6f}, "
        f"baseline_delta={result.metrics.baseline_delta:.6f}, "
        f"aggregate_impressions={result.metrics.aggregate_impressions}, "
        f"aggregate_clicks={result.metrics.aggregate_clicks}, "
        f"baseline_ci_width={result.metrics.baseline_ci_width:.6f}, "
        f"baseline_guard={result.metrics.baseline_guard_reason}, "
        f"max_delta={result.metrics.max_absolute_weight_delta:.6f}, "
        f"redis_write={result.redis_write_applied}, "
        f"postgres_write={result.postgres_write_applied}, "
        f"snapshot_name={result.snapshot_name or 'not_generated'}, "
        "snapshot_id="
        f"{result.postgres_snapshot_id if result.postgres_snapshot_id is not None else 'n/a'}"
    )


if __name__ == "__main__":
    main()
