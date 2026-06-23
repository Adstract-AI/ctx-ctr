from __future__ import annotations

import math

import pytest

from ctx_ctr.models.seed import (
    SeedBucketStatistic,
    SeedModelMetrics,
    SeedModelSnapshot,
    SeedWeights,
)
from ctx_ctr.models.weight_update import (
    RedisBucketScanResult,
    WeightUpdateResult,
    WeightUpdateRunConfig,
    WeightUpdateRunMetrics,
)
from ctx_ctr.services.weight_update_service import WeightUpdateService


class FakeRedisStore:
    def __init__(self, snapshot: SeedModelSnapshot, buckets: list[SeedBucketStatistic]) -> None:
        self.current_snapshot = snapshot
        self.scan_result = RedisBucketScanResult(
            scanned_key_count=len(buckets),
            valid_bucket_count=len(buckets),
            invalid_bucket_count=0,
            buckets=buckets,
        )
        self.written_snapshot: SeedModelSnapshot | None = None

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        return self.scan_result

    def read_current_weights(self) -> SeedModelSnapshot:
        return self.current_snapshot

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        self.written_snapshot = snapshot


class FakePostgresWriter:
    def __init__(self) -> None:
        self.inserted: list[tuple[SeedModelSnapshot, WeightUpdateRunMetrics]] = []

    def insert_model_snapshot(
        self,
        snapshot: SeedModelSnapshot,
        metrics: WeightUpdateRunMetrics,
    ) -> int:
        self.inserted.append((snapshot, metrics))
        return 101


def test_only_trusted_buckets_are_used() -> None:
    snapshot = build_snapshot()
    trusted_bucket = build_bucket(ctr=0.12, trusted=True)
    untrusted_bucket = build_bucket(
        ad_category="travel",
        publisher_domain="tech.example",
        conversation_category="gaming",
        ctr=0.9,
        clicks=90,
        impressions=100,
        trusted=False,
    )

    trusted_only = run_service(snapshot=snapshot, buckets=[trusted_bucket], dry_run=True)
    with_untrusted = run_service(
        snapshot=snapshot,
        buckets=[trusted_bucket, untrusted_bucket],
        dry_run=True,
    )

    assert trusted_only.model_snapshot.weights == with_untrusted.model_snapshot.weights
    assert with_untrusted.metrics.trusted_bucket_count == 1
    assert with_untrusted.metrics.skipped_bucket_count == 1


def test_unknown_feature_values_are_initialized_safely() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(
        ad_category="education",
        publisher_domain="blog.example",
        conversation_category="wellness",
        ctr=0.08,
        clicks=16,
        impressions=200,
        trusted=True,
    )

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True)

    assert result.metrics.unknown_feature_count == 3
    assert "education" in result.model_snapshot.weights.w_ad
    assert "blog.example" in result.model_snapshot.weights.w_dom
    assert "wellness" in result.model_snapshot.weights.w_ctx


def test_w0_stays_unchanged_and_feature_weights_can_change() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.14, clicks=28, impressions=200, trusted=True)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True)

    assert result.model_snapshot.w0 == snapshot.w0
    assert result.metrics.w0_unchanged is True
    assert result.model_snapshot.weights.w_ad != snapshot.weights.w_ad
    assert result.model_snapshot.weights.w_dom != snapshot.weights.w_dom
    assert result.model_snapshot.weights.w_ctx != snapshot.weights.w_ctx


def test_weight_families_remain_centered_after_updates() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.11, clicks=22, impressions=200, trusted=True)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True)

    for family in [
        result.model_snapshot.weights.w_ad,
        result.model_snapshot.weights.w_dom,
        result.model_snapshot.weights.w_ctx,
    ]:
        assert math.isclose(sum(family.values()) / len(family), 0.0, abs_tol=1e-12)


def test_ridge_regularization_reduces_the_updated_weight() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.18, clicks=36, impressions=200, trusted=True)

    without_ridge = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, ridge=0.0)
    with_ridge = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, ridge=0.1)

    assert (
        with_ridge.model_snapshot.weights.w_ad["finance"]
        < without_ridge.model_snapshot.weights.w_ad["finance"]
    )


def test_max_delta_clipping_is_applied_before_centering() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.9, clicks=180, impressions=200, trusted=True)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, max_delta=0.01)

    assert result.metrics.max_absolute_weight_delta == pytest.approx(0.01)


def test_dry_run_does_not_write_redis_or_postgres() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.13, clicks=26, impressions=200, trusted=True)
    redis = FakeRedisStore(snapshot, [bucket])
    postgres = FakePostgresWriter()
    config = build_config(dry_run=True)

    result = WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)

    assert result.redis_write_applied is False
    assert result.postgres_write_applied is False
    assert redis.written_snapshot is None
    assert postgres.inserted == []


def test_real_run_writes_redis_and_postgres() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(ctr=0.13, clicks=26, impressions=200, trusted=True)
    redis = FakeRedisStore(snapshot, [bucket])
    postgres = FakePostgresWriter()
    config = build_config(dry_run=False)

    result = WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)

    assert result.redis_write_applied is True
    assert result.postgres_write_applied is True
    assert result.postgres_snapshot_id == 101
    assert redis.written_snapshot == result.model_snapshot
    assert len(postgres.inserted) == 1


def build_snapshot() -> SeedModelSnapshot:
    return SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=-3.8918202981106265,
        weights=SeedWeights(
            w_ad={"finance": 0.1, "travel": -0.1},
            w_dom={"news.example": 0.05, "tech.example": -0.05},
            w_ctx={"personal_finance": 0.08, "gaming": -0.08},
        ),
        metrics=SeedModelMetrics(baseline_ctr=0.02, prior_strength=100.0),
    )


def build_bucket(
    *,
    ad_category: str = "finance",
    publisher_domain: str = "news.example",
    conversation_category: str = "personal_finance",
    impressions: int = 200,
    clicks: int = 20,
    ctr: float = 0.1,
    trusted: bool = True,
) -> SeedBucketStatistic:
    alpha_prior = 2.0
    beta_prior = 98.0
    alpha_posterior = alpha_prior + clicks
    beta_posterior = beta_prior + impressions - clicks
    ci_low = max(0.0, ctr - 0.01)
    ci_high = min(1.0, ctr + 0.01)
    return SeedBucketStatistic(
        ad_category=ad_category,
        publisher_domain=publisher_domain,
        conversation_category=conversation_category,
        impressions=impressions,
        clicks=clicks,
        alpha_prior=alpha_prior,
        beta_prior=beta_prior,
        alpha_posterior=alpha_posterior,
        beta_posterior=beta_posterior,
        ctr=ctr,
        variance=0.0001,
        ci_low=ci_low,
        ci_high=ci_high,
        trusted=trusted,
    )


def build_config(
    *,
    dry_run: bool,
    ridge: float = 0.01,
    max_delta: float = 0.25,
) -> WeightUpdateRunConfig:
    return WeightUpdateRunConfig(
        learning_rate=0.25,
        evidence_smoothing=1000.0,
        ridge=ridge,
        max_delta=max_delta,
        min_trusted_buckets=1,
        snapshot_name_prefix="flink_weight_update",
        dry_run=dry_run,
    )


def run_service(
    *,
    snapshot: SeedModelSnapshot,
    buckets: list[SeedBucketStatistic],
    dry_run: bool,
    ridge: float = 0.01,
    max_delta: float = 0.25,
) -> WeightUpdateResult:
    redis = FakeRedisStore(snapshot, buckets)
    postgres = FakePostgresWriter()
    config = build_config(dry_run=dry_run, ridge=ridge, max_delta=max_delta)
    return WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)
