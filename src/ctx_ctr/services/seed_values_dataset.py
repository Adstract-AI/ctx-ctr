"""Deterministic seed-values dataset."""

from __future__ import annotations

from datetime import UTC, datetime

from ctx_ctr.models.seed import (
    SeedBucketStatistic,
    SeedDataset,
    SeedModelMetrics,
    SeedModelSnapshot,
    SeedRunSummary,
    SeedWeights,
)
from ctx_ctr.services.ctr_math import beta_variance, clipped_confidence_interval, logit, sigmoid

AD_CATEGORIES = ["finance", "travel", "education", "health", "gaming"]
PUBLISHER_DOMAINS = ["news.example", "tech.example", "lifestyle.example", "sports.example"]
CONVERSATION_CATEGORIES = [
    "personal_finance",
    "trip_planning",
    "online_learning",
    "wellness",
    "gaming",
]

BASELINE_CTR = 0.02
PRIOR_STRENGTH = 100.0
Z_SCORE_95 = 1.96
TRUSTED_MIN_IMPRESSIONS = 100
TRUSTED_MAX_VARIANCE = 0.0005
TRUSTED_MAX_CI_WIDTH = 0.05
SEED_GENERATED_AT = datetime(2026, 1, 1, tzinfo=UTC)

AD_WEIGHTS = {
    "finance": 0.18,
    "travel": 0.08,
    "education": 0.02,
    "health": -0.06,
    "gaming": -0.22,
}
DOMAIN_WEIGHTS = {
    "news.example": 0.05,
    "tech.example": 0.12,
    "lifestyle.example": -0.03,
    "sports.example": -0.14,
}
CONTEXT_WEIGHTS = {
    "personal_finance": 0.16,
    "trip_planning": 0.09,
    "online_learning": 0.02,
    "wellness": -0.04,
    "gaming": -0.23,
}


def build_seed_values_dataset() -> SeedDataset:
    """Build the deterministic seed-values dataset."""

    w0 = logit(BASELINE_CTR)
    snapshot = SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=w0,
        weights=SeedWeights(w_ad=AD_WEIGHTS, w_dom=DOMAIN_WEIGHTS, w_ctx=CONTEXT_WEIGHTS),
        metrics=SeedModelMetrics(
            baseline_ctr=BASELINE_CTR,
            prior_strength=PRIOR_STRENGTH,
        ),
    )
    buckets = _build_bucket_statistics(w0)
    run_summary = _build_seed_run_summary(buckets)
    return SeedDataset(
        generated_at=SEED_GENERATED_AT,
        bucket_statistics=buckets,
        model_snapshots=[snapshot],
        run_summaries=[run_summary],
    )


def _build_bucket_statistics(w0: float) -> list[SeedBucketStatistic]:
    buckets: list[SeedBucketStatistic] = []
    for ad_index, ad_category in enumerate(AD_CATEGORIES):
        for domain_index, publisher_domain in enumerate(PUBLISHER_DOMAINS):
            for context_index, conversation_category in enumerate(CONVERSATION_CATEGORIES):
                impression_seed = (
                    120
                    + (ad_index * 37)
                    + (domain_index * 29)
                    + (context_index * 19)
                    + ((ad_index + domain_index + context_index) % 5) * 11
                )
                weight_sum = (
                    w0
                    + AD_WEIGHTS[ad_category]
                    + DOMAIN_WEIGHTS[publisher_domain]
                    + CONTEXT_WEIGHTS[conversation_category]
                )
                prior_mean = sigmoid(weight_sum)
                expected_clicks = round(impression_seed * prior_mean)
                click_adjustment = (ad_index * 2 + domain_index + context_index) % 3 - 1
                clicks = max(0, min(impression_seed, expected_clicks + click_adjustment))
                buckets.append(
                    _build_bucket_statistic(
                        ad_category=ad_category,
                        publisher_domain=publisher_domain,
                        conversation_category=conversation_category,
                        impressions=impression_seed,
                        clicks=clicks,
                        prior_mean=prior_mean,
                    )
                )
    return buckets


def _build_bucket_statistic(
    *,
    ad_category: str,
    publisher_domain: str,
    conversation_category: str,
    impressions: int,
    clicks: int,
    prior_mean: float,
) -> SeedBucketStatistic:
    alpha_prior = prior_mean * PRIOR_STRENGTH
    beta_prior = (1.0 - prior_mean) * PRIOR_STRENGTH
    alpha_posterior = alpha_prior + clicks
    beta_posterior = beta_prior + impressions - clicks
    ctr = alpha_posterior / (alpha_posterior + beta_posterior)
    variance = beta_variance(alpha_posterior, beta_posterior)
    ci_low, ci_high = clipped_confidence_interval(ctr, variance, Z_SCORE_95)
    trusted = (
        impressions >= TRUSTED_MIN_IMPRESSIONS
        and variance <= TRUSTED_MAX_VARIANCE
        and (ci_high - ci_low) <= TRUSTED_MAX_CI_WIDTH
    )
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
        variance=variance,
        ci_low=ci_low,
        ci_high=ci_high,
        trusted=trusted,
    )


def _build_seed_run_summary(buckets: list[SeedBucketStatistic]) -> SeedRunSummary:
    impressions = sum(bucket.impressions for bucket in buckets)
    clicks = sum(bucket.clicks for bucket in buckets)
    trusted = sum(1 for bucket in buckets if bucket.trusted)
    return SeedRunSummary(
        summary_name="seed_values_baseline",
        config={
            "ad_categories": AD_CATEGORIES,
            "publisher_domains": PUBLISHER_DOMAINS,
            "conversation_categories": CONVERSATION_CATEGORIES,
            "prior_strength": PRIOR_STRENGTH,
            "baseline_ctr": BASELINE_CTR,
        },
        metrics={
            "bucket_count": len(buckets),
            "trusted_bucket_count": trusted,
            "total_impressions": impressions,
            "total_clicks": clicks,
            "observed_ctr": clicks / impressions,
        },
    )
