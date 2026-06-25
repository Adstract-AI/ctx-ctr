"""Task 2 service for periodic feature-family weight recalibration."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Protocol

from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.logging_config import get_logger
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
from ctx_ctr.services.ctr_math import (
    beta_variance,
    clip_value,
    clipped_confidence_interval,
    safe_logit,
    sigmoid,
)

logger = get_logger(__name__)
FEATURE_UPDATE_Z_SCORE = 1.645
BASELINE_UPDATE_Z_SCORE = 1.645


class WeightUpdateRedisStore(Protocol):
    """Redis operations required by the Task 2 weight-update service."""

    def scan_bucket_statistics(self) -> RedisBucketScanResult: ...
    def read_current_weights(self) -> SeedModelSnapshot: ...
    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None: ...


class WeightUpdatePostgresWriter(Protocol):
    """PostgreSQL operations required by the Task 2 weight-update service."""

    def insert_model_snapshot(
        self,
        snapshot: SeedModelSnapshot,
        metrics: WeightUpdateRunMetrics,
    ) -> int: ...


@dataclass
class SingleFeatureBucket:
    """Aggregated evidence for one value within a weight family."""

    value: str
    impressions: int = 0
    clicks: int = 0

    def add(self, impressions: int, clicks: int) -> None:
        """Add triplet-bucket evidence to this single-feature bucket."""

        self.impressions += impressions
        self.clicks += clicks


@dataclass(frozen=True)
class FeatureFamilyUpdateResult:
    """Updated weights and metrics for one feature family."""

    weights: dict[str, float]
    bucket_count: int
    updated_count: int
    skipped_count: int
    insufficient_impression_count: int
    wide_ci_count: int
    unknown_feature_count: int
    max_absolute_delta: float


@dataclass(frozen=True)
class BaselineUpdateProposal:
    """Computed global baseline update and audit values for one run."""

    old_w0: float
    new_w0: float
    baseline_delta: float
    update_enabled: bool
    update_applied: bool
    aggregate_impressions: int
    aggregate_clicks: int
    aggregate_observed_ctr: float
    posterior_baseline_ctr: float
    ci_low: float
    ci_high: float
    ci_width: float
    guard_reason: str
    valid_bucket_count: int
    invalid_bucket_count: int


class WeightUpdateService:
    """Learn feature-family weights and the global baseline from Redis bucket statistics."""

    def __init__(
        self,
        redis_store: WeightUpdateRedisStore,
        postgres_writer: WeightUpdatePostgresWriter | None = None,
    ) -> None:
        self._redis_store = redis_store
        self._postgres_writer = postgres_writer

    def recalibrate(self, config: WeightUpdateRunConfig) -> WeightUpdateResult:
        """Run one recalibration pass from Redis to Redis/PostgreSQL."""

        scan_result = self._redis_store.scan_bucket_statistics()
        current_model = self._redis_store.read_current_weights()
        baseline_proposal = self._build_baseline_update_proposal(
            current_model=current_model,
            buckets=scan_result.buckets,
            scan_invalid_bucket_count=scan_result.invalid_bucket_count,
            config=config,
        )
        feature_buckets, invalid_feature_triplet_count = self._aggregate_single_feature_buckets(
            scan_result.buckets
        )

        logger.info(
            f"Weight update bucket scan: scanned={scan_result.scanned_key_count}, "
            f"valid={scan_result.valid_bucket_count}, invalid={scan_result.invalid_bucket_count}"
        )
        logger.info(
            "Single-feature buckets generated: "
            f"ad={len(feature_buckets[0])}, domain={len(feature_buckets[1])}, "
            f"context={len(feature_buckets[2])}, "
            f"invalid_triplets_for_features={invalid_feature_triplet_count}"
        )
        logger.info(
            f"Baseline update enabled={config.baseline_update}, "
            f"aggregate_impressions={baseline_proposal.aggregate_impressions}, "
            f"aggregate_clicks={baseline_proposal.aggregate_clicks}, "
            f"aggregate_observed_ctr={baseline_proposal.aggregate_observed_ctr:.6f}, "
            f"ci_width={baseline_proposal.ci_width:.6f}, "
            f"delta={baseline_proposal.baseline_delta:.6f}, "
            f"guard={baseline_proposal.guard_reason}"
        )

        if config.baseline_update and not baseline_proposal.update_applied:
            empty_feature_metrics = self._empty_feature_metrics(feature_buckets)
            metrics = self._build_metrics(
                scan_result=scan_result,
                feature_metrics=empty_feature_metrics,
                invalid_feature_triplet_count=invalid_feature_triplet_count,
                max_absolute_weight_delta=0.0,
                config=config,
                baseline_proposal=baseline_proposal,
            )
            logger.warning(
                f"Skipping weight update because baseline guard failed: "
                f"{baseline_proposal.guard_reason}"
            )
            return WeightUpdateResult(
                accepted=False,
                dry_run=config.dry_run,
                model_snapshot=current_model,
                metrics=metrics,
                redis_write_applied=False,
                postgres_write_applied=False,
                skipped_reason=f"baseline_guard_failed:{baseline_proposal.guard_reason}",
            )

        ad_result = self._update_feature_family(
            current_weights=current_model.weights.w_ad,
            buckets=feature_buckets[0],
            w0=current_model.w0,
            prior_strength=current_model.metrics.family_prior_strengths["ad"],
            config=config,
        )
        domain_result = self._update_feature_family(
            current_weights=current_model.weights.w_dom,
            buckets=feature_buckets[1],
            w0=current_model.w0,
            prior_strength=current_model.metrics.family_prior_strengths["domain"],
            config=config,
        )
        context_result = self._update_feature_family(
            current_weights=current_model.weights.w_ctx,
            buckets=feature_buckets[2],
            w0=current_model.w0,
            prior_strength=current_model.metrics.family_prior_strengths["context"],
            config=config,
        )
        feature_metrics = (ad_result, domain_result, context_result)
        updated_feature_bucket_count = sum(result.updated_count for result in feature_metrics)

        if updated_feature_bucket_count == 0 and not baseline_proposal.update_applied:
            skipped_baseline_proposal = self._without_accepted_baseline_update(
                baseline_proposal
            )
            metrics = self._build_metrics(
                scan_result=scan_result,
                feature_metrics=feature_metrics,
                invalid_feature_triplet_count=invalid_feature_triplet_count,
                max_absolute_weight_delta=0.0,
                config=config,
                baseline_proposal=skipped_baseline_proposal,
            )
            logger.warning("Skipping weight update because no single-feature buckets passed guards")
            return WeightUpdateResult(
                accepted=False,
                dry_run=config.dry_run,
                model_snapshot=current_model,
                metrics=metrics,
                redis_write_applied=False,
                postgres_write_applied=False,
                skipped_reason="no_feature_buckets_passed_guards",
            )

        snapshot_name = self._build_snapshot_name(config.snapshot_name_prefix)
        updated_metrics = SeedModelMetrics(
            baseline_ctr=(
                sigmoid(baseline_proposal.new_w0)
                if config.baseline_update
                else current_model.metrics.baseline_ctr
            ),
            triplet_prior_strength=current_model.metrics.triplet_prior_strength,
            global_prior_strength=current_model.metrics.global_prior_strength,
            family_prior_strengths=current_model.metrics.family_prior_strengths,
        )
        updated_snapshot = SeedModelSnapshot(
            snapshot_name=snapshot_name,
            w0=baseline_proposal.new_w0,
            weights=SeedWeights(
                w_ad=ad_result.weights,
                w_dom=domain_result.weights,
                w_ctx=context_result.weights,
            ),
            metrics=updated_metrics,
        )
        max_absolute_weight_delta = max(
            ad_result.max_absolute_delta,
            domain_result.max_absolute_delta,
            context_result.max_absolute_delta,
        )
        metrics = self._build_metrics(
            scan_result=scan_result,
            feature_metrics=feature_metrics,
            invalid_feature_triplet_count=invalid_feature_triplet_count,
            max_absolute_weight_delta=max_absolute_weight_delta,
            config=config,
            baseline_proposal=baseline_proposal,
        )

        redis_write_applied = False
        postgres_write_applied = False
        postgres_snapshot_id: int | None = None

        if config.dry_run:
            logger.info(f"Prepared dry-run weight update snapshot {snapshot_name}")
        else:
            self._redis_store.write_current_weights(updated_snapshot)
            redis_write_applied = True
            if self._postgres_writer is None:
                raise WeightUpdateError("A PostgreSQL writer is required for non-dry-run updates")
            postgres_snapshot_id = self._postgres_writer.insert_model_snapshot(
                updated_snapshot,
                metrics,
            )
            postgres_write_applied = True
            logger.info(
                f"Applied weight update snapshot {snapshot_name} "
                f"with PostgreSQL id {postgres_snapshot_id}"
            )

        logger.info(
            f"Weight update max absolute delta before centering: {max_absolute_weight_delta:.6f}"
        )
        return WeightUpdateResult(
            accepted=True,
            dry_run=config.dry_run,
            model_snapshot=updated_snapshot,
            metrics=metrics,
            redis_write_applied=redis_write_applied,
            postgres_write_applied=postgres_write_applied,
            snapshot_name=snapshot_name,
            postgres_snapshot_id=postgres_snapshot_id,
        )

    def _build_baseline_update_proposal(
        self,
        *,
        current_model: SeedModelSnapshot,
        buckets: list[SeedBucketStatistic],
        scan_invalid_bucket_count: int,
        config: WeightUpdateRunConfig,
    ) -> BaselineUpdateProposal:
        """Compute the optional global baseline update from valid bucket evidence."""

        valid_buckets = [
            bucket
            for bucket in buckets
            if bucket.impressions > 0 and bucket.clicks <= bucket.impressions
        ]
        parsed_invalid_bucket_count = len(buckets) - len(valid_buckets)
        aggregate_impressions = sum(bucket.impressions for bucket in valid_buckets)
        aggregate_clicks = sum(bucket.clicks for bucket in valid_buckets)
        aggregate_observed_ctr = (
            aggregate_clicks / aggregate_impressions if aggregate_impressions > 0 else 0.0
        )

        old_w0 = current_model.w0
        old_baseline_ctr = sigmoid(old_w0)
        prior_strength = current_model.metrics.global_prior_strength
        alpha_prior = old_baseline_ctr * prior_strength
        beta_prior = (1.0 - old_baseline_ctr) * prior_strength
        alpha_posterior = alpha_prior + aggregate_clicks
        beta_posterior = beta_prior + aggregate_impressions - aggregate_clicks
        posterior_baseline_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
        variance = beta_variance(alpha_posterior, beta_posterior)
        ci_low, ci_high = clipped_confidence_interval(
            posterior_baseline_ctr,
            variance,
            BASELINE_UPDATE_Z_SCORE,
        )
        ci_width = ci_high - ci_low

        if not config.baseline_update:
            return BaselineUpdateProposal(
                old_w0=old_w0,
                new_w0=old_w0,
                baseline_delta=0.0,
                update_enabled=False,
                update_applied=False,
                aggregate_impressions=aggregate_impressions,
                aggregate_clicks=aggregate_clicks,
                aggregate_observed_ctr=aggregate_observed_ctr,
                posterior_baseline_ctr=posterior_baseline_ctr,
                ci_low=ci_low,
                ci_high=ci_high,
                ci_width=ci_width,
                guard_reason="baseline_update_disabled",
                valid_bucket_count=len(valid_buckets),
                invalid_bucket_count=scan_invalid_bucket_count + parsed_invalid_bucket_count,
            )

        if ci_width > config.baseline_max_ci_width:
            return BaselineUpdateProposal(
                old_w0=old_w0,
                new_w0=old_w0,
                baseline_delta=0.0,
                update_enabled=True,
                update_applied=False,
                aggregate_impressions=aggregate_impressions,
                aggregate_clicks=aggregate_clicks,
                aggregate_observed_ctr=aggregate_observed_ctr,
                posterior_baseline_ctr=posterior_baseline_ctr,
                ci_low=ci_low,
                ci_high=ci_high,
                ci_width=ci_width,
                guard_reason="baseline_ci_width_too_wide",
                valid_bucket_count=len(valid_buckets),
                invalid_bucket_count=scan_invalid_bucket_count + parsed_invalid_bucket_count,
            )

        new_w0 = safe_logit(posterior_baseline_ctr)
        delta = new_w0 - old_w0
        return BaselineUpdateProposal(
            old_w0=old_w0,
            new_w0=new_w0,
            baseline_delta=delta,
            update_enabled=True,
            update_applied=True,
            aggregate_impressions=aggregate_impressions,
            aggregate_clicks=aggregate_clicks,
            aggregate_observed_ctr=aggregate_observed_ctr,
            posterior_baseline_ctr=posterior_baseline_ctr,
            ci_low=ci_low,
            ci_high=ci_high,
            ci_width=ci_width,
            guard_reason="baseline_guards_passed",
            valid_bucket_count=len(valid_buckets),
            invalid_bucket_count=scan_invalid_bucket_count + parsed_invalid_bucket_count,
        )

    def _without_accepted_baseline_update(
        self,
        baseline_proposal: BaselineUpdateProposal,
    ) -> BaselineUpdateProposal:
        """Return baseline metrics for a run where no model snapshot is accepted."""

        return replace(
            baseline_proposal,
            new_w0=baseline_proposal.old_w0,
            baseline_delta=0.0,
            update_applied=False,
        )

    def _aggregate_single_feature_buckets(
        self,
        triplet_buckets: list[SeedBucketStatistic],
    ) -> tuple[
        tuple[
            dict[str, SingleFeatureBucket],
            dict[str, SingleFeatureBucket],
            dict[str, SingleFeatureBucket],
        ],
        int,
    ]:
        """Aggregate triplet CTR buckets into single-feature family buckets."""

        ad_buckets: dict[str, SingleFeatureBucket] = {}
        domain_buckets: dict[str, SingleFeatureBucket] = {}
        context_buckets: dict[str, SingleFeatureBucket] = {}
        invalid_triplet_count = 0

        for bucket in triplet_buckets:
            if bucket.impressions <= 0 or bucket.clicks > bucket.impressions:
                invalid_triplet_count += 1
                continue

            ad_buckets.setdefault(
                bucket.ad_category,
                SingleFeatureBucket(value=bucket.ad_category),
            ).add(bucket.impressions, bucket.clicks)
            domain_buckets.setdefault(
                bucket.publisher_domain,
                SingleFeatureBucket(value=bucket.publisher_domain),
            ).add(bucket.impressions, bucket.clicks)
            context_buckets.setdefault(
                bucket.conversation_category,
                SingleFeatureBucket(value=bucket.conversation_category),
            ).add(bucket.impressions, bucket.clicks)

        return (ad_buckets, domain_buckets, context_buckets), invalid_triplet_count

    def _update_feature_family(
        self,
        *,
        current_weights: dict[str, float],
        buckets: dict[str, SingleFeatureBucket],
        w0: float,
        prior_strength: float,
        config: WeightUpdateRunConfig,
    ) -> FeatureFamilyUpdateResult:
        """Update one feature family using single-feature Bayesian buckets."""

        updated_weights = dict(current_weights)
        unknown_feature_count = 0
        updated_count = 0
        insufficient_impression_count = 0
        wide_ci_count = 0
        max_absolute_delta = 0.0

        for feature_value in buckets:
            if feature_value not in updated_weights:
                updated_weights[feature_value] = 0.0
                unknown_feature_count += 1

        for feature_value, bucket in buckets.items():
            current_weight = updated_weights[feature_value]
            posterior_ctr, ci_width = self._single_feature_posterior(
                bucket=bucket,
                current_weight=current_weight,
                w0=w0,
                prior_strength=prior_strength,
            )

            if bucket.impressions < config.min_feature_impressions:
                insufficient_impression_count += 1
                continue
            if ci_width > config.max_feature_ci_width:
                wide_ci_count += 1
                continue

            z_target = safe_logit(posterior_ctr)
            delta_star = z_target - w0
            eta = (
                config.learning_rate
                * bucket.impressions
                / (bucket.impressions + config.evidence_smoothing)
            )
            raw_delta = eta * (delta_star - current_weight)
            regularized_delta = raw_delta - config.ridge * current_weight
            delta = clip_value(regularized_delta, -config.max_delta, config.max_delta)
            updated_weights[feature_value] = current_weight + delta
            max_absolute_delta = max(max_absolute_delta, abs(delta))
            updated_count += 1

        skipped_count = insufficient_impression_count + wide_ci_count
        return FeatureFamilyUpdateResult(
            weights=self._center_family(updated_weights),
            bucket_count=len(buckets),
            updated_count=updated_count,
            skipped_count=skipped_count,
            insufficient_impression_count=insufficient_impression_count,
            wide_ci_count=wide_ci_count,
            unknown_feature_count=unknown_feature_count,
            max_absolute_delta=max_absolute_delta,
        )

    def _single_feature_posterior(
        self,
        *,
        bucket: SingleFeatureBucket,
        current_weight: float,
        w0: float,
        prior_strength: float,
    ) -> tuple[float, float]:
        """Return posterior CTR and CI width for one single-feature bucket."""

        prior_mean = sigmoid(w0 + current_weight)
        alpha_prior = prior_mean * prior_strength
        beta_prior = (1.0 - prior_mean) * prior_strength
        alpha_posterior = alpha_prior + bucket.clicks
        beta_posterior = beta_prior + bucket.impressions - bucket.clicks
        posterior_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
        variance = beta_variance(alpha_posterior, beta_posterior)
        ci_low, ci_high = clipped_confidence_interval(
            posterior_ctr,
            variance,
            FEATURE_UPDATE_Z_SCORE,
        )
        return posterior_ctr, ci_high - ci_low

    def _empty_feature_metrics(
        self,
        feature_buckets: tuple[
            dict[str, SingleFeatureBucket],
            dict[str, SingleFeatureBucket],
            dict[str, SingleFeatureBucket],
        ],
    ) -> tuple[FeatureFamilyUpdateResult, FeatureFamilyUpdateResult, FeatureFamilyUpdateResult]:
        """Return no-op feature metrics when the run is skipped before feature learning."""

        return (
            self._empty_family_result(len(feature_buckets[0])),
            self._empty_family_result(len(feature_buckets[1])),
            self._empty_family_result(len(feature_buckets[2])),
        )

    def _empty_family_result(self, bucket_count: int) -> FeatureFamilyUpdateResult:
        """Return no-op metrics for one family."""

        return FeatureFamilyUpdateResult(
            weights={},
            bucket_count=bucket_count,
            updated_count=0,
            skipped_count=bucket_count,
            insufficient_impression_count=0,
            wide_ci_count=0,
            unknown_feature_count=0,
            max_absolute_delta=0.0,
        )

    def _center_family(self, weights: dict[str, float]) -> dict[str, float]:
        """Return a copy of one family with zero mean preserved."""

        if not weights:
            raise WeightUpdateError("Cannot center an empty weight family")

        mean_weight = sum(weights.values()) / len(weights)
        return {feature_value: weight - mean_weight for feature_value, weight in weights.items()}

    def _build_metrics(
        self,
        *,
        scan_result: RedisBucketScanResult,
        feature_metrics: tuple[
            FeatureFamilyUpdateResult,
            FeatureFamilyUpdateResult,
            FeatureFamilyUpdateResult,
        ],
        invalid_feature_triplet_count: int,
        max_absolute_weight_delta: float,
        config: WeightUpdateRunConfig,
        baseline_proposal: BaselineUpdateProposal,
    ) -> WeightUpdateRunMetrics:
        """Build the metrics payload recorded for the current run."""

        ad_result, domain_result, context_result = feature_metrics
        return WeightUpdateRunMetrics(
            input_bucket_count=scan_result.scanned_key_count,
            valid_bucket_count=scan_result.valid_bucket_count,
            invalid_bucket_count=scan_result.invalid_bucket_count + invalid_feature_triplet_count,
            ad_feature_bucket_count=ad_result.bucket_count,
            domain_feature_bucket_count=domain_result.bucket_count,
            context_feature_bucket_count=context_result.bucket_count,
            updated_feature_bucket_count=(
                ad_result.updated_count
                + domain_result.updated_count
                + context_result.updated_count
            ),
            skipped_feature_bucket_count=(
                ad_result.skipped_count
                + domain_result.skipped_count
                + context_result.skipped_count
            ),
            insufficient_impression_feature_count=(
                ad_result.insufficient_impression_count
                + domain_result.insufficient_impression_count
                + context_result.insufficient_impression_count
            ),
            wide_ci_feature_count=(
                ad_result.wide_ci_count + domain_result.wide_ci_count + context_result.wide_ci_count
            ),
            unknown_feature_count=(
                ad_result.unknown_feature_count
                + domain_result.unknown_feature_count
                + context_result.unknown_feature_count
            ),
            max_absolute_weight_delta=max_absolute_weight_delta,
            learning_rate=config.learning_rate,
            evidence_smoothing=config.evidence_smoothing,
            ridge=config.ridge,
            max_delta=config.max_delta,
            min_feature_impressions=config.min_feature_impressions,
            max_feature_ci_width=config.max_feature_ci_width,
            dry_run=config.dry_run,
            w0_unchanged=baseline_proposal.new_w0 == baseline_proposal.old_w0,
            old_w0=baseline_proposal.old_w0,
            new_w0=baseline_proposal.new_w0,
            baseline_delta=baseline_proposal.baseline_delta,
            baseline_update_enabled=baseline_proposal.update_enabled,
            baseline_update_applied=baseline_proposal.update_applied,
            aggregate_impressions=baseline_proposal.aggregate_impressions,
            aggregate_clicks=baseline_proposal.aggregate_clicks,
            aggregate_observed_ctr=baseline_proposal.aggregate_observed_ctr,
            posterior_baseline_ctr=baseline_proposal.posterior_baseline_ctr,
            baseline_ci_low=baseline_proposal.ci_low,
            baseline_ci_high=baseline_proposal.ci_high,
            baseline_ci_width=baseline_proposal.ci_width,
            baseline_guard_reason=baseline_proposal.guard_reason,
            valid_baseline_bucket_count=baseline_proposal.valid_bucket_count,
            invalid_baseline_bucket_count=baseline_proposal.invalid_bucket_count,
        )

    def _build_snapshot_name(self, snapshot_name_prefix: str) -> str:
        """Return the generated snapshot name for the current run."""

        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        return f"{snapshot_name_prefix}_{timestamp}"
