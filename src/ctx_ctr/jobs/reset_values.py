"""Command entrypoint for resetting seeded values."""

from __future__ import annotations

import argparse
import json

from ctx_ctr.adapters.postgres_seed import PostgresSeedAdapter
from ctx_ctr.adapters.redis_seed import RedisSeedAdapter
from ctx_ctr.config import load_settings
from ctx_ctr.logging_config import configure_logging
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedRunSummary
from ctx_ctr.services.seed_service import SeedService


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
    """Parse CLI arguments and reset demo seed state."""

    parser = argparse.ArgumentParser(description="Reset deterministic CTR values.")
    parser.add_argument("--dry-run", action="store_true", help="validate reset without writing")
    args = parser.parse_args()

    settings = load_settings()
    configure_logging(settings.log_level, settings.log_color)
    if args.dry_run:
        service = SeedService(
            postgres=DryRunPostgresResetAdapter(),
            redis=DryRunRedisResetAdapter(),
        )
        result = service.reset(dry_run=True)
        print(json.dumps(result.model_dump(), indent=2, sort_keys=True))
        return

    with PostgresSeedAdapter(settings.postgres_dsn) as postgres:
        redis = RedisSeedAdapter(settings.redis_url)
        service = SeedService(postgres=postgres, redis=redis)
        result = service.reset(dry_run=False)

    print(json.dumps(result.model_dump(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
