"""Helpers for Flink-native streaming weight updates."""

from __future__ import annotations

from dataclasses import dataclass

from ctx_ctr.constants import (
    DEFAULT_CTR_TRUST_MAX_CI_WIDTH,
    DEFAULT_CTR_TRUST_MAX_VARIANCE,
    DEFAULT_CTR_TRUST_MIN_IMPRESSIONS,
    DEFAULT_CTR_TRUST_Z_SCORE,
)
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot
from ctx_ctr.models.streaming_weight_update import (
    StreamingTripletCounter,
    StreamingWeightEvidenceSnapshot,
    StreamingWeightEvidenceState,
)
from ctx_ctr.services.ctr_math import beta_variance, clipped_confidence_interval, sigmoid


@dataclass(frozen=True)
class StreamingWeightUpdateEvidenceBuilder:
    """Build weight-update bucket evidence from cumulative stream state."""

    def apply_event(
        self,
        state: StreamingWeightEvidenceState,
        event: CtrEvent,
    ) -> StreamingWeightEvidenceState:
        """Return updated cumulative evidence after one valid event."""

        key = event.kafka_key().decode("utf-8")
        existing = state.triplets.get(key)
        counter = existing or StreamingTripletCounter(
            ad_category=event.ad_category,
            publisher_domain=event.publisher_domain,
            conversation_category=event.conversation_category,
        )
        updated_counter = counter.model_copy(
            update={
                "impressions": counter.impressions + int(event.event_type == "impression"),
                "clicks": counter.clicks + int(event.event_type == "click"),
            }
        )
        return state.model_copy(
            update={
                "triplets": state.triplets | {key: updated_counter},
                "processed_events": state.processed_events + 1,
            }
        )

    def mark_invalid_event(self, state: StreamingWeightEvidenceState) -> StreamingWeightEvidenceState:
        """Return updated cumulative evidence after one invalid event payload."""

        return state.model_copy(update={"invalid_events": state.invalid_events + 1})

    def build_snapshot(
        self,
        *,
        state: StreamingWeightEvidenceState,
        current_model: SeedModelSnapshot,
    ) -> StreamingWeightEvidenceSnapshot:
        """Build SeedBucketStatistic evidence from current stream counters."""

        buckets: list[SeedBucketStatistic] = []
        invalid_bucket_count = 0
        for counter in state.triplets.values():
            if counter.impressions <= 0 or counter.clicks > counter.impressions:
                invalid_bucket_count += 1
                continue
            buckets.append(self._build_bucket(counter=counter, current_model=current_model))

        return StreamingWeightEvidenceSnapshot(
            scanned_key_count=len(state.triplets),
            valid_bucket_count=len(buckets),
            invalid_bucket_count=invalid_bucket_count + state.invalid_events,
            buckets=buckets,
        )

    def _build_bucket(
        self,
        *,
        counter: StreamingTripletCounter,
        current_model: SeedModelSnapshot,
    ) -> SeedBucketStatistic:
        prior_mean = sigmoid(
            current_model.w0
            + current_model.weights.w_ad.get(counter.ad_category, 0.0)
            + current_model.weights.w_dom.get(counter.publisher_domain, 0.0)
            + current_model.weights.w_ctx.get(counter.conversation_category, 0.0)
        )
        prior_strength = current_model.metrics.triplet_prior_strength
        alpha_prior = prior_mean * prior_strength
        beta_prior = (1.0 - prior_mean) * prior_strength
        alpha_posterior = alpha_prior + counter.clicks
        beta_posterior = beta_prior + counter.impressions - counter.clicks
        ctr = alpha_posterior / (alpha_posterior + beta_posterior)
        variance = beta_variance(alpha_posterior, beta_posterior)
        ci_low, ci_high = clipped_confidence_interval(
            ctr,
            variance,
            DEFAULT_CTR_TRUST_Z_SCORE,
        )
        trusted = (
            counter.impressions >= DEFAULT_CTR_TRUST_MIN_IMPRESSIONS
            and variance <= DEFAULT_CTR_TRUST_MAX_VARIANCE
            and (ci_high - ci_low) <= DEFAULT_CTR_TRUST_MAX_CI_WIDTH
        )
        return SeedBucketStatistic(
            ad_category=counter.ad_category,
            publisher_domain=counter.publisher_domain,
            conversation_category=counter.conversation_category,
            impressions=counter.impressions,
            clicks=counter.clicks,
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
