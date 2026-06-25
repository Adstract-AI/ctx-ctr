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
GLOBAL_PRIOR_STRENGTH = 500.0
AD_PRIOR_STRENGTH = 200.0
DOMAIN_PRIOR_STRENGTH = 200.0
CONTEXT_PRIOR_STRENGTH = 150.0
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


def build_seed_values_dataset(
    *,
    baseline_prior_mean: float = BASELINE_CTR,
    triplet_prior_strength: float = PRIOR_STRENGTH,
    global_prior_strength: float = GLOBAL_PRIOR_STRENGTH,
    ad_prior_strength: float = AD_PRIOR_STRENGTH,
    domain_prior_strength: float = DOMAIN_PRIOR_STRENGTH,
    context_prior_strength: float = CONTEXT_PRIOR_STRENGTH,
) -> SeedDataset:
    """Build the deterministic seed-values dataset."""

    w0 = logit(baseline_prior_mean)
    snapshot = SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=w0,
        weights=SeedWeights(w_ad=AD_WEIGHTS, w_dom=DOMAIN_WEIGHTS, w_ctx=CONTEXT_WEIGHTS),
        metrics=SeedModelMetrics(
            baseline_ctr=baseline_prior_mean,
            global_prior_strength=global_prior_strength,
            prior_strength=triplet_prior_strength,
            family_prior_strengths={
                "ad": ad_prior_strength,
                "domain": domain_prior_strength,
                "context": context_prior_strength,
            },
        ),
    )
    buckets = _build_bucket_statistics(w0, triplet_prior_strength)
    run_summary = _build_seed_run_summary(
        buckets=buckets,
        baseline_prior_mean=baseline_prior_mean,
        triplet_prior_strength=triplet_prior_strength,
        global_prior_strength=global_prior_strength,
        ad_prior_strength=ad_prior_strength,
        domain_prior_strength=domain_prior_strength,
        context_prior_strength=context_prior_strength,
    )
    return SeedDataset(
        generated_at=SEED_GENERATED_AT,
        bucket_statistics=buckets,
        model_snapshots=[snapshot],
        run_summaries=[run_summary],
    )


def _build_bucket_statistics(w0: float, triplet_prior_strength: float) -> list[SeedBucketStatistic]:
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
                        prior_strength=triplet_prior_strength,
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
    prior_strength: float,
) -> SeedBucketStatistic:
    alpha_prior = prior_mean * prior_strength
    beta_prior = (1.0 - prior_mean) * prior_strength
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


def _build_seed_run_summary(
    *,
    buckets: list[SeedBucketStatistic],
    baseline_prior_mean: float,
    triplet_prior_strength: float,
    global_prior_strength: float,
    ad_prior_strength: float,
    domain_prior_strength: float,
    context_prior_strength: float,
) -> SeedRunSummary:
    impressions = sum(bucket.impressions for bucket in buckets)
    clicks = sum(bucket.clicks for bucket in buckets)
    trusted = sum(1 for bucket in buckets if bucket.trusted)
    return SeedRunSummary(
        summary_name="seed_values_baseline",
        config={
            "ad_categories": AD_CATEGORIES,
            "publisher_domains": PUBLISHER_DOMAINS,
            "conversation_categories": CONVERSATION_CATEGORIES,
            "baseline_prior_mean": baseline_prior_mean,
            "triplet_prior_strength": triplet_prior_strength,
            "global_prior_strength": global_prior_strength,
            "ad_prior_strength": ad_prior_strength,
            "domain_prior_strength": domain_prior_strength,
            "context_prior_strength": context_prior_strength,
        },
        metrics={
            "bucket_count": len(buckets),
            "trusted_bucket_count": trusted,
            "total_impressions": impressions,
            "total_clicks": clicks,
            "observed_ctr": clicks / impressions,
        },
    )
