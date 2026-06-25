"""Realtime Bayesian CTR state transition service."""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel, ConfigDict

from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.ctr_state import (
    CtrBucketKey,
    CtrBucketStatistic,
    JsonPayload,
    CtrModelSnapshot,
    DeadLetterPayload,
)
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.services.ctr_math import beta_variance, clipped_confidence_interval, sigmoid
from ctx_ctr.services.seed_values_dataset import (
    TRUSTED_MAX_CI_WIDTH,
    TRUSTED_MAX_VARIANCE,
    TRUSTED_MIN_IMPRESSIONS,
    Z_SCORE_95,
)

logger = get_logger(__name__)


class CtrUpdateResult(BaseModel):
    """Result of applying one event to bucket state."""

    bucket: CtrBucketStatistic | None
    dead_letter: DeadLetterPayload | None
    valid_event: bool

    model_config = ConfigDict(frozen=True)


class RealtimeCtrUpdateService:
    """Apply realtime CTR events to Bayesian bucket statistics."""

    def __init__(self, model: CtrModelSnapshot) -> None:
        self._model = model

    def apply_event(
        self,
        event: CtrEvent,
        existing_bucket: CtrBucketStatistic | None,
    ) -> CtrUpdateResult:
        """Apply one event to existing or freshly initialized bucket state."""

        bucket = existing_bucket or self.initialize_bucket(event_bucket_key(event))
        impressions = bucket.impressions
        clicks = bucket.clicks

        if event.event_type == "impression":
            impressions += 1
        else:
            if clicks + 1 > impressions:
                logger.debug(
                    "Rejected click event because clicks would exceed impressions "
                    f"for bucket {bucket.bucket_key.kafka_key}"
                )
                return CtrUpdateResult(
                    bucket=None,
                    dead_letter=DeadLetterPayload(
                        reason="clicks_would_exceed_impressions",
                        event=cast(JsonPayload, event.json_payload()),
                    ),
                    valid_event=False,
                )
            clicks += 1

        return CtrUpdateResult(
            bucket=self._recompute_bucket(bucket, impressions=impressions, clicks=clicks),
            dead_letter=None,
            valid_event=True,
        )

    def initialize_bucket(self, bucket_key: CtrBucketKey) -> CtrBucketStatistic:
        """Build an empty bucket from current model weights."""

        prior_mean = sigmoid(
            self._model.w0
            + self._model.weights.w_ad.get(bucket_key.ad_category, 0.0)
            + self._model.weights.w_dom.get(bucket_key.publisher_domain, 0.0)
            + self._model.weights.w_ctx.get(bucket_key.conversation_category, 0.0)
        )
        alpha_prior = prior_mean * self._model.metrics.triplet_prior_strength
        beta_prior = (1.0 - prior_mean) * self._model.metrics.triplet_prior_strength
        return self._build_bucket(
            bucket_key=bucket_key,
            impressions=0,
            clicks=0,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
        )

    def _recompute_bucket(
        self,
        bucket: CtrBucketStatistic,
        *,
        impressions: int,
        clicks: int,
    ) -> CtrBucketStatistic:
        return self._build_bucket(
            bucket_key=bucket.bucket_key,
            impressions=impressions,
            clicks=clicks,
            alpha_prior=bucket.alpha_prior,
            beta_prior=bucket.beta_prior,
        )

    def _build_bucket(
        self,
        *,
        bucket_key: CtrBucketKey,
        impressions: int,
        clicks: int,
        alpha_prior: float,
        beta_prior: float,
    ) -> CtrBucketStatistic:
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
        return CtrBucketStatistic(
            ad_category=bucket_key.ad_category,
            publisher_domain=bucket_key.publisher_domain,
            conversation_category=bucket_key.conversation_category,
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


def event_bucket_key(event: CtrEvent) -> CtrBucketKey:
    """Return the bucket key for one event."""

    return CtrBucketKey(
        ad_category=event.ad_category,
        publisher_domain=event.publisher_domain,
        conversation_category=event.conversation_category,
    )
