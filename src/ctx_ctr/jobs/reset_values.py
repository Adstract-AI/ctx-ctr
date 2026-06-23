"""Command entrypoint for resetting seeded values."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.postgres_seed import PostgresSeedAdapter
from ctx_ctr.adapters.redis_seed import RedisSeedAdapter
from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL, POSTGRES_DSN, REDIS_URL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.job_configs import ResetValuesJobConfig
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedRunSummary
from ctx_ctr.services.seed_service import SeedService

logger = get_logger("ctx_ctr.jobs.reset_values")
DEFAULT_CONFIG_PATH = "configs/reset_values.yaml"


class DryRunPostgresResetAdapter:
    """No-op Postgres adapter used for offline reset validation."""

    def reset_seed_tables(self) -> None:
        """Skip Postgres reset."""

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Skip bucket writes."""

    def insert_model_snapshots(self, snapshots: list[SeedModelSnapshot]) -> None:
        """Skip snapshot writes."""

    def insert_seed_run_summaries(self, summaries: list[SeedRunSummary]) -> None:
        """Skip seed run-summary writes."""


class DryRunRedisResetAdapter:
    """No-op Redis adapter used for offline reset validation."""

    def reset_seed_keys(self) -> None:
        """Skip Redis reset."""

    def write_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Skip bucket writes."""

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        """Skip current-weight writes."""


def main() -> None:
    """Parse CLI arguments and reset seeded values."""

    parser = argparse.ArgumentParser(description="Reset deterministic CTR values.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="validate reset without writing",
    )
    args = parser.parse_args()
    file_config = load_job_config(args.config, ResetValuesJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    if config.dry_run:
        logger.info("Running reset values dry-run")
        service = SeedService(
            postgres=DryRunPostgresResetAdapter(),
            redis=DryRunRedisResetAdapter(),
        )
        result = service.reset(dry_run=True)
        print_success(
            "Reset values dry-run",
            [
                f"postgres reset planned: {result.reset_postgres}",
                f"redis reset planned: {result.reset_redis}",
                f"config: {args.config}",
            ],
        )
        return

    try:
        logger.info("Starting reset of Postgres seed tables and Redis seed keys")
        with PostgresSeedAdapter(POSTGRES_DSN) as postgres:
            redis = RedisSeedAdapter(REDIS_URL)
            service = SeedService(postgres=postgres, redis=redis)
            result = service.reset(dry_run=False)
    except Exception as error:
        print_failure("Reset values", error)
        raise

    print_success(
        "Reset values complete",
        [
            f"postgres reset: {result.reset_postgres}",
            f"redis reset: {result.reset_redis}",
            f"config: {args.config}",
        ],
    )


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    if args.dry_run is not None:
        overrides["dry_run"] = args.dry_run
    return overrides


if __name__ == "__main__":
    main()
