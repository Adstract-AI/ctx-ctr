"""Command entrypoint for deterministic value seeding."""

from __future__ import annotations

import argparse

from ctx_ctr.adapters.postgres_seed import PostgresSeedAdapter
from ctx_ctr.adapters.redis_seed import RedisSeedAdapter
from ctx_ctr.env_variables import LOG_COLOR, LOG_LEVEL, POSTGRES_DSN, REDIS_URL
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.job_configs import SeedValuesJobConfig
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedRunSummary
from ctx_ctr.services.seed_values_dataset import build_seed_values_dataset
from ctx_ctr.services.seed_service import SeedService

logger = get_logger("ctx_ctr.jobs.seed_values")
DEFAULT_CONFIG_PATH = "configs/seed_values.yaml"


class DryRunPostgresSeedAdapter:
    """No-op Postgres adapter used for offline dry-run validation."""

    def reset_seed_tables(self) -> None:
        """Skip Postgres reset."""

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Skip bucket writes."""

    def insert_model_snapshots(self, snapshots: list[SeedModelSnapshot]) -> None:
        """Skip snapshot writes."""

    def insert_seed_run_summaries(self, summaries: list[SeedRunSummary]) -> None:
        """Skip seed run-summary writes."""


class DryRunRedisSeedAdapter:
    """No-op Redis adapter used for offline dry-run validation."""

    def reset_seed_keys(self) -> None:
        """Skip Redis reset."""

    def write_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Skip bucket writes."""

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        """Skip current-weight writes."""


def main() -> None:
    """Parse CLI arguments and run the value seeder."""

    parser = argparse.ArgumentParser(description="Seed deterministic CTR values.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="validate and print planned seed counts only",
    )
    parser.add_argument("--baseline-prior-mean", type=float, default=None)
    parser.add_argument("--triplet-prior-strength", type=float, default=None)
    parser.add_argument("--global-prior-strength", type=float, default=None)
    parser.add_argument("--ad-prior-strength", type=float, default=None)
    parser.add_argument("--domain-prior-strength", type=float, default=None)
    parser.add_argument("--context-prior-strength", type=float, default=None)
    args = parser.parse_args()
    file_config = load_job_config(args.config, SeedValuesJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    dataset = build_seed_values_dataset(
        baseline_prior_mean=config.baseline_prior_mean,
        triplet_prior_strength=config.triplet_prior_strength,
        global_prior_strength=config.global_prior_strength,
        ad_prior_strength=config.ad_prior_strength,
        domain_prior_strength=config.domain_prior_strength,
        context_prior_strength=config.context_prior_strength,
    )
    configure_logging(LOG_LEVEL, LOG_COLOR)
    logger.info(
        f"Prepared seed values dataset: {dataset.summary()['bucket_statistics']} buckets, "
        f"{dataset.summary()['model_snapshots']} model snapshots, "
        f"{dataset.summary()['run_summaries']} run summaries"
    )
    if config.dry_run:
        logger.info("Running seed values dry-run")
        service = SeedService(postgres=DryRunPostgresSeedAdapter(), redis=DryRunRedisSeedAdapter())
        result = service.seed(
            dataset,
            dry_run=True,
        )
        print_success(
            "Seed values dry-run",
            [
                f"bucket statistics: {result.summary['bucket_statistics']}",
                f"model snapshots: {result.summary['model_snapshots']}",
                f"run summaries: {result.summary['run_summaries']}",
                f"total impressions: {result.summary['total_impressions']}",
                f"total clicks: {result.summary['total_clicks']}",
                f"config: {args.config}",
            ],
        )
        return

    try:
        logger.info("Starting seed values write to Postgres and Redis")
        with PostgresSeedAdapter(POSTGRES_DSN) as postgres:
            redis = RedisSeedAdapter(REDIS_URL)
            service = SeedService(postgres=postgres, redis=redis)
            result = service.seed(
                dataset,
                dry_run=config.dry_run,
            )
    except Exception as error:
        print_failure("Seed values", error)
        raise

    print_success(
        "Seed values complete",
        [
            f"bucket statistics: {result.summary['bucket_statistics']}",
            f"model snapshots: {result.summary['model_snapshots']}",
            f"run summaries: {result.summary['run_summaries']}",
            f"total impressions: {result.summary['total_impressions']}",
            f"total clicks: {result.summary['total_clicks']}",
            f"config: {args.config}",
        ],
    )


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "dry_run",
        "baseline_prior_mean",
        "triplet_prior_strength",
        "global_prior_strength",
        "ad_prior_strength",
        "domain_prior_strength",
        "context_prior_strength",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


if __name__ == "__main__":
    main()
