from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ctx_ctr.models.seed import (
    SeedBucketStatistic,
    SeedDataset,
    SeedModelMetrics,
    SeedModelSnapshot,
    SeedRunSummary,
    SeedWeights,
)


def test_seed_bucket_rejects_clicks_above_impressions() -> None:
    with pytest.raises(ValidationError):
        SeedBucketStatistic(
            ad_category="finance",
            publisher_domain="news.example",
            conversation_category="personal_finance",
            impressions=10,
            clicks=11,
            alpha_prior=2.0,
            beta_prior=98.0,
            alpha_posterior=13.0,
            beta_posterior=97.0,
            ctr=0.1,
            variance=0.001,
            ci_low=0.01,
            ci_high=0.2,
            trusted=False,
        )


def test_seed_dataset_rejects_duplicate_buckets() -> None:
    bucket = SeedBucketStatistic(
        ad_category="finance",
        publisher_domain="news.example",
        conversation_category="personal_finance",
        impressions=10,
        clicks=1,
        alpha_prior=2.0,
        beta_prior=98.0,
        alpha_posterior=3.0,
        beta_posterior=107.0,
        ctr=3.0 / 110.0,
        variance=0.0002,
        ci_low=0.0,
        ci_high=0.06,
        trusted=False,
    )
    snapshot = SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=-3.89,
        weights=SeedWeights(w_ad={"finance": 0.0}, w_dom={"news.example": 0.0}, w_ctx={"ctx": 0.0}),
        metrics=SeedModelMetrics(
            baseline_ctr=0.02,
            global_prior_strength=500.0,
            prior_strength=100.0,
            family_prior_strengths={"ad": 200.0, "domain": 200.0, "context": 150.0},
        ),
    )
    summary = SeedRunSummary(
        summary_name="seed_values_baseline",
        config={},
        metrics={},
    )

    with pytest.raises(ValidationError):
        SeedDataset(
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
            bucket_statistics=[bucket, bucket],
            model_snapshots=[snapshot],
            run_summaries=[summary],
        )
