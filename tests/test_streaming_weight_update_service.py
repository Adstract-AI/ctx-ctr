from __future__ import annotations

from datetime import UTC, datetime

from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.seed import SeedModelMetrics, SeedModelSnapshot, SeedWeights
from ctx_ctr.models.streaming_weight_update import StreamingWeightEvidenceState
from ctx_ctr.services.streaming_weight_update_service import StreamingWeightUpdateEvidenceBuilder


def test_streaming_weight_evidence_builder_accumulates_events_into_triplets() -> None:
    builder = StreamingWeightUpdateEvidenceBuilder()
    state = StreamingWeightEvidenceState()

    state = builder.apply_event(state, build_event(event_type="impression"))
    state = builder.apply_event(state, build_event(event_type="click"))

    counter = state.triplets["finance:news.example:personal_finance"]
    assert state.processed_events == 2
    assert counter.impressions == 1
    assert counter.clicks == 1


def test_streaming_weight_evidence_builder_builds_valid_bucket_snapshot() -> None:
    builder = StreamingWeightUpdateEvidenceBuilder()
    state = StreamingWeightEvidenceState()
    state = builder.apply_event(state, build_event(event_type="impression"))
    state = builder.apply_event(state, build_event(event_type="click"))

    snapshot = builder.build_snapshot(state=state, current_model=build_model())

    assert snapshot.scanned_key_count == 1
    assert snapshot.valid_bucket_count == 1
    assert snapshot.invalid_bucket_count == 0
    assert snapshot.buckets[0].impressions == 1
    assert snapshot.buckets[0].clicks == 1
    assert snapshot.buckets[0].clicks <= snapshot.buckets[0].impressions


def test_streaming_weight_evidence_builder_counts_invalid_payloads_and_buckets() -> None:
    builder = StreamingWeightUpdateEvidenceBuilder()
    state = StreamingWeightEvidenceState()
    state = builder.mark_invalid_event(state)
    state = builder.apply_event(state, build_event(event_type="click"))

    snapshot = builder.build_snapshot(state=state, current_model=build_model())

    assert snapshot.scanned_key_count == 1
    assert snapshot.valid_bucket_count == 0
    assert snapshot.invalid_bucket_count == 2


def build_event(*, event_type: str) -> CtrEvent:
    return CtrEvent(
        event_id=f"{event_type}-1",
        event_type=event_type,  # type: ignore[arg-type]
        ad_category="finance",
        publisher_domain="news.example",
        conversation_category="personal_finance",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        impression_event_id="impression-1" if event_type == "click" else None,
    )


def build_model() -> SeedModelSnapshot:
    return SeedModelSnapshot(
        snapshot_name="seed_values_v1",
        w0=-3.8918202981106265,
        weights=SeedWeights(
            w_ad={"finance": 0.0},
            w_dom={"news.example": 0.0},
            w_ctx={"personal_finance": 0.0},
        ),
        metrics=SeedModelMetrics(
            baseline_ctr=0.02,
            global_prior_strength=500.0,
            triplet_prior_strength=100.0,
            family_prior_strengths={"ad": 200.0, "domain": 200.0, "context": 150.0},
        ),
    )
