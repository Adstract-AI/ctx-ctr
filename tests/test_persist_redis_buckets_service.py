from __future__ import annotations

from ctx_ctr.models.seed import SeedBucketStatistic
from ctx_ctr.models.weight_update import RedisBucketScanResult
from ctx_ctr.services.persist_redis_buckets_service import PersistRedisBucketsService


class FakeRedisSource:
    def __init__(self, buckets: list[SeedBucketStatistic], invalid_count: int = 0) -> None:
        self.result = RedisBucketScanResult(
            scanned_key_count=len(buckets) + invalid_count,
            valid_bucket_count=len(buckets),
            invalid_bucket_count=invalid_count,
            buckets=buckets,
        )

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        return self.result


class FakePostgresWriter:
    def __init__(self) -> None:
        self.buckets: list[SeedBucketStatistic] = []

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        self.buckets.extend(buckets)


def test_persist_redis_buckets_dry_run_scans_without_writing() -> None:
    bucket = build_bucket()
    redis = FakeRedisSource([bucket], invalid_count=1)
    postgres = FakePostgresWriter()

    result = PersistRedisBucketsService(redis, postgres).persist(dry_run=True)

    assert result.dry_run is True
    assert result.scanned_key_count == 2
    assert result.valid_bucket_count == 1
    assert result.invalid_bucket_count == 1
    assert result.persisted_bucket_count == 0
    assert result.postgres_write_applied is False
    assert postgres.buckets == []


def test_persist_redis_buckets_writes_valid_buckets_to_postgres() -> None:
    bucket = build_bucket()
    redis = FakeRedisSource([bucket])
    postgres = FakePostgresWriter()

    result = PersistRedisBucketsService(redis, postgres).persist(dry_run=False)

    assert result.persisted_bucket_count == 1
    assert result.postgres_write_applied is True
    assert postgres.buckets == [bucket]


def test_persist_redis_buckets_with_no_valid_buckets_does_not_write() -> None:
    redis = FakeRedisSource([], invalid_count=2)
    postgres = FakePostgresWriter()

    result = PersistRedisBucketsService(redis, postgres).persist(dry_run=False)

    assert result.scanned_key_count == 2
    assert result.valid_bucket_count == 0
    assert result.invalid_bucket_count == 2
    assert result.persisted_bucket_count == 0
    assert result.postgres_write_applied is False
    assert postgres.buckets == []


def build_bucket() -> SeedBucketStatistic:
    return SeedBucketStatistic(
        ad_category="finance",
        publisher_domain="news.example",
        conversation_category="personal_finance",
        impressions=100,
        clicks=2,
        alpha_prior=2.0,
        beta_prior=98.0,
        alpha_posterior=4.0,
        beta_posterior=196.0,
        ctr=0.02,
        variance=0.0001,
        ci_low=0.0,
        ci_high=0.04,
        trusted=True,
    )
