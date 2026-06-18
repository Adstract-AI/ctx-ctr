import math

from ctx_ctr.services.seed_values_dataset import (
    AD_CATEGORIES,
    CONVERSATION_CATEGORIES,
    PRIOR_STRENGTH,
    PUBLISHER_DOMAINS,
    build_seed_values_dataset,
)


def test_seed_values_dataset_has_expected_shape() -> None:
    dataset = build_seed_values_dataset()

    assert len(dataset.bucket_statistics) == (
        len(AD_CATEGORIES) * len(PUBLISHER_DOMAINS) * len(CONVERSATION_CATEGORIES)
    )
    assert len(dataset.model_snapshots) == 1
    assert len(dataset.experiment_results) == 1
    assert dataset.model_snapshots[0].snapshot_name == "seed_values_v1"


def test_seed_values_bucket_statistics_match_beta_binomial_formula() -> None:
    bucket = build_seed_values_dataset().bucket_statistics[0]

    assert bucket.clicks <= bucket.impressions
    assert math.isclose(bucket.alpha_prior + bucket.beta_prior, PRIOR_STRENGTH)
    assert math.isclose(bucket.alpha_posterior, bucket.alpha_prior + bucket.clicks)
    assert math.isclose(
        bucket.beta_posterior,
        bucket.beta_prior + bucket.impressions - bucket.clicks,
    )
    assert math.isclose(
        bucket.ctr,
        bucket.alpha_posterior / (bucket.alpha_posterior + bucket.beta_posterior),
    )
    assert 0 <= bucket.ci_low <= bucket.ci_high <= 1


def test_seed_values_weights_are_centered_by_family() -> None:
    snapshot = build_seed_values_dataset().model_snapshots[0]

    for weights in [
        snapshot.weights.w_ad,
        snapshot.weights.w_dom,
        snapshot.weights.w_ctx,
    ]:
        assert isinstance(weights, dict)
        assert math.isclose(
            sum(float(value) for value in weights.values()) / len(weights),
            0.0,
            abs_tol=1e-12,
        )
