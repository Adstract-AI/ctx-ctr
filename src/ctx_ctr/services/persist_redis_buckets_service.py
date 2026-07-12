"""Service for persisting Redis CTR bucket state into PostgreSQL."""

from __future__ import annotations

from typing import Protocol

from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.persist_redis_buckets import PersistRedisBucketsResult
from ctx_ctr.models.seed import SeedBucketStatistic
from ctx_ctr.models.weight_update import RedisBucketScanResult

logger = get_logger(__name__)


class RedisBucketSource(Protocol):
    """Read bucket statistics from Redis."""

    def scan_bucket_statistics(self) -> RedisBucketScanResult: ...


class PostgresBucketWriter(Protocol):
    """Persist bucket statistics into PostgreSQL."""

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None: ...


class PersistRedisBucketsService:
    """Persist valid Redis CTR buckets into PostgreSQL."""

    def __init__(
        self,
        redis_source: RedisBucketSource,
        postgres_writer: PostgresBucketWriter | None,
    ) -> None:
        self._redis_source = redis_source
        self._postgres_writer = postgres_writer

    def persist(self, *, dry_run: bool) -> PersistRedisBucketsResult:
        """Scan Redis buckets and upsert valid payloads into PostgreSQL."""

        scan_result = self._redis_source.scan_bucket_statistics()
        logger.info(
            f"Redis bucket persistence scan: scanned={scan_result.scanned_key_count}, "
            f"valid={scan_result.valid_bucket_count}, invalid={scan_result.invalid_bucket_count}"
        )

        postgres_write_applied = False
        persisted_bucket_count = 0
        if not dry_run and scan_result.buckets:
            if self._postgres_writer is None:
                raise ValueError("Postgres writer is required when dry_run is false")
            self._postgres_writer.upsert_bucket_statistics(scan_result.buckets)
            postgres_write_applied = True
            persisted_bucket_count = len(scan_result.buckets)
            logger.info(f"Persisted {persisted_bucket_count} Redis CTR buckets into Postgres")

        if dry_run:
            logger.info("Redis bucket persistence dry-run completed without Postgres writes")
        elif not scan_result.buckets:
            logger.info("No valid Redis CTR buckets found to persist")

        return PersistRedisBucketsResult(
            dry_run=dry_run,
            scanned_key_count=scan_result.scanned_key_count,
            valid_bucket_count=scan_result.valid_bucket_count,
            invalid_bucket_count=scan_result.invalid_bucket_count,
            persisted_bucket_count=persisted_bucket_count,
            postgres_write_applied=postgres_write_applied,
        )
