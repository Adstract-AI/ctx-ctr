from __future__ import annotations

from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedRunSummary
from ctx_ctr.services.seed_values_dataset import build_seed_values_dataset
from ctx_ctr.services.seed_service import SeedService


class FakePostgresSeedAdapter:
    def __init__(self) -> None:
        self.reset_called = False
        self.buckets: list[SeedBucketStatistic] = []
        self.snapshots: list[SeedModelSnapshot] = []
        self.summaries: list[SeedRunSummary] = []

    def reset_seed_tables(self) -> None:
        self.reset_called = True

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        self.buckets.extend(buckets)

    def insert_model_snapshots(self, snapshots: list[SeedModelSnapshot]) -> None:
        self.snapshots.extend(snapshots)

    def insert_seed_run_summaries(self, summaries: list[SeedRunSummary]) -> None:
        self.summaries.extend(summaries)


class FakeRedisSeedAdapter:
    def __init__(self) -> None:
        self.reset_called = False
        self.buckets: list[SeedBucketStatistic] = []
        self.current_weights: SeedModelSnapshot | None = None

    def reset_seed_keys(self) -> None:
        self.reset_called = True

    def write_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        self.buckets.extend(buckets)

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        self.current_weights = snapshot


def test_seed_service_dry_run_does_not_write() -> None:
    postgres = FakePostgresSeedAdapter()
    redis = FakeRedisSeedAdapter()
    dataset = build_seed_values_dataset()

    result = SeedService(postgres, redis).seed(
        dataset,
        dry_run=True,
    )

    assert result.dry_run is True
    assert postgres.reset_called is False
    assert redis.reset_called is False
    assert postgres.buckets == []
    assert redis.buckets == []


def test_seed_service_resets_and_writes_to_postgres_and_redis() -> None:
    postgres = FakePostgresSeedAdapter()
    redis = FakeRedisSeedAdapter()
    dataset = build_seed_values_dataset()

    result = SeedService(postgres, redis).seed(
        dataset,
        dry_run=False,
    )

    assert result.summary["bucket_statistics"] == len(dataset.bucket_statistics)
    assert postgres.reset_called is False
    assert redis.reset_called is False
    assert postgres.buckets == dataset.bucket_statistics
    assert redis.buckets == dataset.bucket_statistics
    assert postgres.snapshots == dataset.model_snapshots
    assert postgres.summaries == dataset.run_summaries
    assert redis.current_weights == dataset.model_snapshots[-1]


def test_seed_service_resets_postgres_and_redis() -> None:
    postgres = FakePostgresSeedAdapter()
    redis = FakeRedisSeedAdapter()

    result = SeedService(postgres, redis).reset(dry_run=False)

    assert result.reset_postgres is True
    assert result.reset_redis is True
    assert postgres.reset_called is True
    assert redis.reset_called is True
    assert postgres.buckets == []
    assert redis.buckets == []


def test_seed_service_reset_dry_run_does_not_write() -> None:
    postgres = FakePostgresSeedAdapter()
    redis = FakeRedisSeedAdapter()

    result = SeedService(postgres, redis).reset(dry_run=True)

    assert result.dry_run is True
    assert postgres.reset_called is False
    assert redis.reset_called is False
