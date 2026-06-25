from __future__ import annotations

from datetime import UTC, datetime

from ctx_ctr.models.ctr_state import CtrBucketKey, CtrModelMetrics, CtrModelSnapshot, CtrModelWeights
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.services.realtime_ctr import RealtimeCtrUpdateService


def test_initialize_bucket_uses_current_model_prior_strength() -> None:
    service = RealtimeCtrUpdateService(build_model())

    bucket = service.initialize_bucket(
        CtrBucketKey(
            ad_category="finance",
            publisher_domain="news.example",
            conversation_category="personal_finance",
        )
    )

    assert bucket.impressions == 0
    assert bucket.clicks == 0
    assert bucket.alpha_prior + bucket.beta_prior == 100.0
    assert bucket.alpha_posterior == bucket.alpha_prior
    assert bucket.beta_posterior == bucket.beta_prior


def test_impression_updates_bucket_and_preserves_click_invariant() -> None:
    service = RealtimeCtrUpdateService(build_model())
    event = build_event(event_type="impression")

    result = service.apply_event(event, existing_bucket=None)

    assert result.valid_event is True
    assert result.dead_letter is None
    assert result.bucket is not None
    assert result.bucket.impressions == 1
    assert result.bucket.clicks == 0
    assert result.bucket.clicks <= result.bucket.impressions


def test_click_without_prior_impression_is_dead_lettered() -> None:
    service = RealtimeCtrUpdateService(build_model())
    event = build_event(event_type="click", impression_event_id="imp-1")

    result = service.apply_event(event, existing_bucket=None)

    assert result.valid_event is False
    assert result.bucket is None
    assert result.dead_letter is not None
    assert result.dead_letter.reason == "clicks_would_exceed_impressions"
    assert result.dead_letter.event["event_id"] == "event-1"


def test_click_after_impression_updates_click_count() -> None:
    service = RealtimeCtrUpdateService(build_model())
    impression_result = service.apply_event(build_event(event_type="impression"), None)
    assert impression_result.bucket is not None

    click_result = service.apply_event(
        build_event(event_type="click", impression_event_id="event-1"),
        impression_result.bucket,
    )

    assert click_result.valid_event is True
    assert click_result.bucket is not None
    assert click_result.bucket.impressions == 1
    assert click_result.bucket.clicks == 1


def build_model() -> CtrModelSnapshot:
    return CtrModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=-3.8918202981106265,
        weights=CtrModelWeights(
            w_ad={"finance": 0.18},
            w_dom={"news.example": 0.05},
            w_ctx={"personal_finance": 0.16},
        ),
        metrics=CtrModelMetrics(
            baseline_ctr=0.02,
            global_prior_strength=500.0,
            prior_strength=100.0,
            family_prior_strengths={"ad": 200.0, "domain": 200.0, "context": 150.0},
        ),
    )


def build_event(
    *,
    event_type: str,
    impression_event_id: str | None = None,
) -> CtrEvent:
    return CtrEvent(
        event_id="event-1",
        event_type=event_type,  # type: ignore[arg-type]
        ad_category="finance",
        publisher_domain="news.example",
        conversation_category="personal_finance",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        impression_event_id=impression_event_id,
    )
