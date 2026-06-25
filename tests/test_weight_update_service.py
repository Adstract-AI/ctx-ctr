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
)
from ctx_ctr.services.ctr_math import (
    beta_variance,
    clipped_confidence_interval,
    safe_logit,
    sigmoid,
)
from ctx_ctr.services.weight_update_service import FEATURE_UPDATE_Z_SCORE, WeightUpdateService


class FakeRedisStore:
    def __init__(
        self,
        snapshot: SeedModelSnapshot,
        buckets: list[SeedBucketStatistic],
        invalid_bucket_count: int = 0,
    ) -> None:
        self.current_snapshot = snapshot
        self.scan_result = RedisBucketScanResult(
            scanned_key_count=len(buckets) + invalid_bucket_count,
            valid_bucket_count=len(buckets),
            invalid_bucket_count=invalid_bucket_count,
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
        self.inserted: list[object] = []

    def insert_model_snapshot(self, snapshot: SeedModelSnapshot, metrics: object) -> int:
        self.inserted.append((snapshot, metrics))
        return 101


def test_triplet_buckets_are_aggregated_into_single_feature_buckets() -> None:
    snapshot = build_snapshot()
    buckets = [
        build_bucket(
            ad_category="finance",
            publisher_domain="news.example",
            conversation_category="personal_finance",
            impressions=600,
            clicks=60,
        ),
        build_bucket(
            ad_category="finance",
            publisher_domain="tech.example",
            conversation_category="gaming",
            impressions=500,
            clicks=50,
        ),
        build_bucket(
            ad_category="travel",
            publisher_domain="news.example",
            conversation_category="gaming",
            impressions=500,
            clicks=25,
        ),
    ]

    result = run_service(snapshot=snapshot, buckets=buckets, dry_run=True, baseline_update=False)

    assert result.metrics.ad_feature_bucket_count == 2
    assert result.metrics.domain_feature_bucket_count == 2
    assert result.metrics.context_feature_bucket_count == 2
    assert result.metrics.updated_feature_bucket_count == 6
    assert result.metrics.skipped_feature_bucket_count == 0


def test_trusted_flag_does_not_control_feature_weight_learning() -> None:
    snapshot = build_snapshot()
    trusted_false_bucket = build_bucket(impressions=1000, clicks=120, trusted=False)

    result = run_service(
        snapshot=snapshot,
        buckets=[trusted_false_bucket],
        dry_run=True,
        baseline_update=False,
    )

    assert result.accepted is True
    assert result.metrics.updated_feature_bucket_count == 3
    assert result.model_snapshot.weights.w_ad != snapshot.weights.w_ad


def test_unknown_feature_values_are_initialized_safely() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(
        ad_category="education",
        publisher_domain="blog.example",
        conversation_category="wellness",
        impressions=1000,
        clicks=80,
    )

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, baseline_update=False)

    assert result.metrics.unknown_feature_count == 3
    assert "education" in result.model_snapshot.weights.w_ad
    assert "blog.example" in result.model_snapshot.weights.w_dom
    assert "wellness" in result.model_snapshot.weights.w_ctx


def test_single_feature_update_math_matches_document_formula() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(
        ad_category="finance",
        publisher_domain="news.example",
        conversation_category="personal_finance",
        impressions=1000,
        clicks=120,
    )
    config = build_config(dry_run=True, baseline_update=False, ridge=0.0, max_delta=10.0)

    result = WeightUpdateService(
        redis_store=FakeRedisStore(snapshot, [bucket]),
        postgres_writer=FakePostgresWriter(),
    ).recalibrate(config)

    old_weight = snapshot.weights.w_ad["finance"]
    prior_mean = sigmoid(snapshot.w0 + old_weight)
    prior_strength = snapshot.metrics.family_prior_strengths["ad"]
    alpha_posterior = prior_mean * prior_strength + bucket.clicks
    beta_posterior = (
        (1.0 - prior_mean) * prior_strength
        + bucket.impressions
        - bucket.clicks
    )
    posterior_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
    delta_star = safe_logit(posterior_ctr) - snapshot.w0
    eta = config.learning_rate * bucket.impressions / (
        bucket.impressions + config.evidence_smoothing
    )
    raw_finance_weight = old_weight + eta * (delta_star - old_weight)
    unchanged_travel_weight = snapshot.weights.w_ad["travel"]
    centered_mean = (raw_finance_weight + unchanged_travel_weight) / 2

    assert result.model_snapshot.weights.w_ad["finance"] == pytest.approx(
        raw_finance_weight - centered_mean
    )


def test_ridge_regularization_reduces_the_updated_weight() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=180)

    without_ridge = run_service(
        snapshot=snapshot,
        buckets=[bucket],
        dry_run=True,
        ridge=0.0,
        max_delta=10.0,
        baseline_update=False,
    )
    with_ridge = run_service(
        snapshot=snapshot,
        buckets=[bucket],
        dry_run=True,
        ridge=0.1,
        max_delta=10.0,
        baseline_update=False,
    )

    assert (
        with_ridge.model_snapshot.weights.w_ad["finance"]
        < without_ridge.model_snapshot.weights.w_ad["finance"]
    )


def test_max_delta_clipping_is_applied_before_centering() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=900)

    result = run_service(
        snapshot=snapshot,
        buckets=[bucket],
        dry_run=True,
        max_delta=0.01,
        baseline_update=False,
    )

    assert result.metrics.max_absolute_weight_delta == pytest.approx(0.01)


def test_feature_buckets_failing_min_impression_guard_do_not_update() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=499, clicks=50)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, baseline_update=False)

    assert result.accepted is False
    assert result.metrics.updated_feature_bucket_count == 0
    assert result.metrics.insufficient_impression_feature_count == 3
    assert result.skipped_reason == "no_feature_buckets_passed_guards"


def test_feature_buckets_failing_ci_guard_do_not_update() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=500, clicks=250)

    result = run_service(
        snapshot=snapshot,
        buckets=[bucket],
        dry_run=True,
        baseline_update=False,
        max_feature_ci_width=0.001,
    )

    assert result.accepted is False
    assert result.metrics.updated_feature_bucket_count == 0
    assert result.metrics.wide_ci_feature_count == 3


def test_invalid_triplet_buckets_are_counted_and_skipped_for_feature_learning() -> None:
    snapshot = build_snapshot()
    valid_bucket = build_bucket(impressions=1000, clicks=120)
    zero_impression_bucket = build_bucket(
        ad_category="travel",
        publisher_domain="tech.example",
        conversation_category="gaming",
        impressions=0,
        clicks=0,
    )

    result = run_service(
        snapshot=snapshot,
        buckets=[valid_bucket, zero_impression_bucket],
        dry_run=True,
        baseline_update=False,
    )

    assert result.metrics.invalid_bucket_count == 1
    assert result.metrics.ad_feature_bucket_count == 1
    assert result.metrics.updated_feature_bucket_count == 3


def test_baseline_update_can_change_w0_and_baseline_ctr() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=80)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True)
    alpha_prior = sigmoid(snapshot.w0) * snapshot.metrics.global_prior_strength
    beta_prior = (1.0 - sigmoid(snapshot.w0)) * snapshot.metrics.global_prior_strength
    posterior_ctr = (alpha_prior + bucket.clicks) / (
        alpha_prior + beta_prior + bucket.impressions
    )

    assert result.model_snapshot.w0 != snapshot.w0
    assert result.model_snapshot.w0 == pytest.approx(safe_logit(posterior_ctr))
    assert result.metrics.w0_unchanged is False
    assert result.metrics.baseline_update_enabled is True
    assert result.metrics.baseline_update_applied is True
    assert result.metrics.baseline_guard_reason == "baseline_guards_passed"
    assert result.model_snapshot.metrics.baseline_ctr == pytest.approx(
        sigmoid(result.model_snapshot.w0)
    )
    assert result.metrics.aggregate_impressions == 1000
    assert result.metrics.aggregate_clicks == 80


def test_baseline_ci_guard_failure_skips_whole_model_write() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=80)
    redis = FakeRedisStore(snapshot, [bucket])
    postgres = FakePostgresWriter()
    config = build_config(dry_run=False, baseline_max_ci_width=0.0001)

    result = WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)

    assert result.accepted is False
    assert result.model_snapshot == snapshot
    assert result.redis_write_applied is False
    assert result.postgres_write_applied is False
    assert redis.written_snapshot is None
    assert postgres.inserted == []
    assert result.skipped_reason == "baseline_guard_failed:baseline_ci_width_too_wide"


def test_weight_families_remain_centered_after_updates() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=110)

    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, baseline_update=False)

    for family in [
        result.model_snapshot.weights.w_ad,
        result.model_snapshot.weights.w_dom,
        result.model_snapshot.weights.w_ctx,
    ]:
        assert math.isclose(sum(family.values()) / len(family), 0.0, abs_tol=1e-12)


def test_dry_run_does_not_write_redis_or_postgres() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=130)
    redis = FakeRedisStore(snapshot, [bucket])
    postgres = FakePostgresWriter()
    config = build_config(dry_run=True)

    result = WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)

    assert result.redis_write_applied is False
    assert result.postgres_write_applied is False
    assert redis.written_snapshot is None
    assert postgres.inserted == []
    assert result.metrics.baseline_update_applied is True


def test_real_run_writes_redis_and_postgres() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=130)
    redis = FakeRedisStore(snapshot, [bucket])
    postgres = FakePostgresWriter()
    config = build_config(dry_run=False)

    result = WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)

    assert result.redis_write_applied is True
    assert result.postgres_write_applied is True
    assert result.postgres_snapshot_id == 101
    assert redis.written_snapshot == result.model_snapshot
    assert len(postgres.inserted) == 1


def test_single_feature_ci_uses_document_90_percent_z_score() -> None:
    snapshot = build_snapshot()
    bucket = build_bucket(impressions=1000, clicks=100)
    result = run_service(snapshot=snapshot, buckets=[bucket], dry_run=True, baseline_update=False)

    prior_mean = sigmoid(snapshot.w0 + snapshot.weights.w_ad["finance"])
    prior_strength = snapshot.metrics.family_prior_strengths["ad"]
    alpha_posterior = prior_mean * prior_strength + bucket.clicks
    beta_posterior = (
        (1.0 - prior_mean) * prior_strength
        + bucket.impressions
        - bucket.clicks
    )
    posterior_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
    variance = beta_variance(alpha_posterior, beta_posterior)
    ci_low, ci_high = clipped_confidence_interval(
        posterior_ctr,
        variance,
        FEATURE_UPDATE_Z_SCORE,
    )

    assert ci_high - ci_low <= result.metrics.max_feature_ci_width


def build_snapshot() -> SeedModelSnapshot:
    return SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=-3.8918202981106265,
        weights=SeedWeights(
            w_ad={"finance": 0.1, "travel": -0.1},
            w_dom={"news.example": 0.05, "tech.example": -0.05},
            w_ctx={"personal_finance": 0.08, "gaming": -0.08},
        ),
        metrics=SeedModelMetrics(
            baseline_ctr=0.02,
            global_prior_strength=500.0,
            triplet_prior_strength=100.0,
            family_prior_strengths={"ad": 200.0, "domain": 200.0, "context": 150.0},
        ),
    )


def build_bucket(
    *,
    ad_category: str = "finance",
    publisher_domain: str = "news.example",
    conversation_category: str = "personal_finance",
    impressions: int = 1000,
    clicks: int = 100,
    trusted: bool = True,
) -> SeedBucketStatistic:
    ctr = clicks / impressions if impressions > 0 else 0.0
    alpha_prior = 2.0
    beta_prior = 98.0
    alpha_posterior = alpha_prior + clicks
    beta_posterior = beta_prior + impressions - clicks
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
        ci_low=max(0.0, ctr - 0.01),
        ci_high=min(1.0, ctr + 0.01),
        trusted=trusted,
    )


def build_config(
    *,
    dry_run: bool,
    ridge: float = 0.01,
    max_delta: float = 0.25,
    min_feature_impressions: int = 500,
    max_feature_ci_width: float = 1.0,
    baseline_update: bool = True,
    baseline_max_ci_width: float = 1.0,
) -> WeightUpdateRunConfig:
    return WeightUpdateRunConfig(
        learning_rate=0.25,
        evidence_smoothing=1000.0,
        ridge=ridge,
        max_delta=max_delta,
        min_feature_impressions=min_feature_impressions,
        max_feature_ci_width=max_feature_ci_width,
        snapshot_name_prefix="flink_weight_update",
        baseline_update=baseline_update,
        baseline_max_ci_width=baseline_max_ci_width,
        dry_run=dry_run,
    )


def run_service(
    *,
    snapshot: SeedModelSnapshot,
    buckets: list[SeedBucketStatistic],
    dry_run: bool,
    ridge: float = 0.01,
    max_delta: float = 0.25,
    min_feature_impressions: int = 500,
    max_feature_ci_width: float = 1.0,
    baseline_update: bool = True,
    baseline_max_ci_width: float = 1.0,
) -> WeightUpdateResult:
    redis = FakeRedisStore(snapshot, buckets)
    postgres = FakePostgresWriter()
    config = build_config(
        dry_run=dry_run,
        ridge=ridge,
        max_delta=max_delta,
        min_feature_impressions=min_feature_impressions,
        max_feature_ci_width=max_feature_ci_width,
        baseline_update=baseline_update,
        baseline_max_ci_width=baseline_max_ci_width,
    )
    return WeightUpdateService(redis_store=redis, postgres_writer=postgres).recalibrate(config)
