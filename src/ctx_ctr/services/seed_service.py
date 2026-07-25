"""Seed orchestration service."""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict

from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import (
    SeedBucketStatistic,
    SeedDataset,
    SeedMetadata,
    SeedModelSnapshot,
    SeedRunSummary,
)

logger = get_logger(__name__)


class SeedPostgresWriter(Protocol):
    """PostgreSQL operations required by the seed service."""

    def reset_seed_tables(self) -> None: ...
    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None: ...
    def insert_model_snapshots(self, snapshots: list[SeedModelSnapshot]) -> None: ...
    def insert_seed_run_summaries(self, summaries: list[SeedRunSummary]) -> None: ...


class SeedRedisWriter(Protocol):
    """Redis operations required by the seed service."""

    def reset_seed_keys(self) -> None: ...
    def write_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None: ...
    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None: ...


class SeedResult(BaseModel):
    """Summary of a seed run."""

    dry_run: bool
    summary: SeedMetadata

    model_config = ConfigDict(frozen=True)


class ResetResult(BaseModel):
    """Summary of a seed-state reset."""

    dry_run: bool
    reset_postgres: bool
    reset_redis: bool

    model_config = ConfigDict(frozen=True)


class SeedService:
    """Coordinate deterministic seed reset and writes across storage adapters."""

    def __init__(
        self,
        postgres: SeedPostgresWriter,
        redis: SeedRedisWriter,
    ) -> None:
        self._postgres = postgres
        self._redis = redis

    def seed(
        self,
        dataset: SeedDataset,
        *,
        dry_run: bool,
    ) -> SeedResult:
        """Validate and apply a complete seed dataset."""

        result = SeedResult(
            dry_run=dry_run,
            summary=dataset.summary(),
        )
        if dry_run:
            logger.debug("Validated seed dataset in dry-run mode")
            return result

        self._postgres.upsert_bucket_statistics(dataset.bucket_statistics)
        self._postgres.insert_model_snapshots(dataset.model_snapshots)
        self._postgres.insert_seed_run_summaries(dataset.run_summaries)
        self._redis.write_bucket_statistics(dataset.bucket_statistics)
        self._redis.write_current_weights(dataset.model_snapshots[-1])
        logger.debug("Seeded seed-values dataset")
        return result

    def reset(self, *, dry_run: bool) -> ResetResult:
        """Clear seed-owned Postgres tables and Redis keys."""

        result = ResetResult(dry_run=dry_run, reset_postgres=True, reset_redis=True)
        if dry_run:
            logger.debug("Validated seed reset in dry-run mode")
            return result

        self._postgres.reset_seed_tables()
        self._redis.reset_seed_keys()
        logger.debug("Reset seed-values state")
        return result
