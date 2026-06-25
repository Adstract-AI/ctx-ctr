"""Command entrypoint for persisting Redis CTR buckets into PostgreSQL."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext

from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.job_configs import PersistRedisBucketsJobConfig
from ctx_ctr.models.persist_redis_buckets import PersistRedisBucketsResult
from ctx_ctr.services.persist_redis_buckets_service import (
    PersistRedisBucketsService,
    PostgresBucketWriter,
    RedisBucketSource,
)

logger = get_logger("ctx_ctr.jobs.persist_redis_buckets")
DEFAULT_CONFIG_PATH = "configs/persist_redis_buckets.yaml"


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and persist Redis CTR bucket state into PostgreSQL."""

    parser = argparse.ArgumentParser(description="Persist Redis CTR bucket state into PostgreSQL.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--redis-url", default=None, help="Redis connection URL")
    parser.add_argument("--postgres-dsn", default=None, help="PostgreSQL DSN")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="scan and validate Redis buckets without writing PostgreSQL",
    )
    args = parser.parse_args(argv)
    file_config = load_job_config(args.config, PersistRedisBucketsJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    logger.info(
        "Configured Redis bucket persistence job: "
        f"redis_url={config.redis_url}, postgres_dsn={config.postgres_dsn}, "
        f"dry_run={config.dry_run}, config={args.config}"
    )

    try:
        redis_source = _build_redis_source(config.redis_url)
        with _open_postgres_writer(config.postgres_dsn, config.dry_run) as postgres_writer:
            service = PersistRedisBucketsService(
                redis_source=redis_source,
                postgres_writer=postgres_writer,
            )
            result = service.persist(dry_run=config.dry_run)
    except Exception as error:
        print_failure("Persist Redis buckets", error)
        raise

    print_success(_result_title(result), [*_result_lines(result), f"config: {args.config}"])


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in ("redis_url", "postgres_dsn", "dry_run"):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


def _build_redis_source(redis_url: str) -> RedisBucketSource:
    """Construct the Redis runtime adapter lazily for easier tests."""

    from ctx_ctr.adapters.redis_runtime import RedisRuntimeAdapter

    return RedisRuntimeAdapter(redis_url)


def _open_postgres_writer(
    postgres_dsn: str,
    dry_run: bool,
) -> AbstractContextManager[PostgresBucketWriter | None]:
    """Open the PostgreSQL runtime adapter only when writes are enabled."""

    if dry_run:
        return nullcontext(None)

    from ctx_ctr.adapters.postgres_runtime import PostgresRuntimeAdapter

    return PostgresRuntimeAdapter(postgres_dsn)


def _result_title(result: PersistRedisBucketsResult) -> str:
    """Return the summary title for the run."""

    if result.dry_run:
        return "Redis bucket persistence dry-run"
    return "Redis bucket persistence complete"


def _result_lines(result: PersistRedisBucketsResult) -> list[str]:
    """Return human-readable result summary lines."""

    return [
        f"redis bucket keys scanned: {result.scanned_key_count}",
        f"valid buckets parsed: {result.valid_bucket_count}",
        f"invalid buckets parsed: {result.invalid_bucket_count}",
        f"persisted buckets: {result.persisted_bucket_count}",
        f"postgres write applied: {result.postgres_write_applied}",
    ]


if __name__ == "__main__":
    main()
