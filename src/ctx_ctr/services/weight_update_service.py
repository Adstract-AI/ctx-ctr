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
class WeightedTargetAggregate:
    """Impression-weighted target accumulator for one feature value."""

    weighted_target_sum: float = 0.0
    impressions: int = 0

    def add(self, target: float, impressions: int) -> None:
        """Add one bucket target weighted by its impression evidence."""

        self.weighted_target_sum += target * impressions
        self.impressions += impressions

    @property
    def weighted_mean(self) -> float:
        """Return the evidence-weighted target mean."""

        if self.impressions == 0:
            raise WeightUpdateError("Cannot compute a weighted mean without impressions")
        return self.weighted_target_sum / self.impressions


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
        trusted_buckets = [bucket for bucket in scan_result.buckets if bucket.trusted]
        skipped_bucket_count = scan_result.valid_bucket_count - len(trusted_buckets)
        baseline_proposal = self._build_baseline_update_proposal(
            current_model=current_model,
            buckets=scan_result.buckets,
            scan_invalid_bucket_count=scan_result.invalid_bucket_count,
            config=config,
        )

        logger.info(
            f"Weight update bucket scan: scanned={scan_result.scanned_key_count}, "
            f"valid={scan_result.valid_bucket_count}, invalid={scan_result.invalid_bucket_count}"
        )
        logger.info(
            f"Weight update trusted buckets: trusted={len(trusted_buckets)}, "
            f"skipped={skipped_bucket_count}"
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

        ad_weights, domain_weights, context_weights, unknown_feature_count = (
            self._prepare_weight_families(current_model, trusted_buckets)
        )

        if len(trusted_buckets) < config.min_trusted_buckets:
            skipped_baseline_proposal = self._without_accepted_baseline_update(
                baseline_proposal
            )
            metrics = self._build_metrics(
                scan_result=scan_result,
                trusted_bucket_count=len(trusted_buckets),
                skipped_bucket_count=skipped_bucket_count,
                unknown_feature_count=unknown_feature_count,
                max_absolute_weight_delta=0.0,
                config=config,
                baseline_proposal=skipped_baseline_proposal,
            )
            logger.warning(
                f"Skipping weight update because trusted buckets "
                f"({len(trusted_buckets)}) are below the minimum {config.min_trusted_buckets}"
            )
            return WeightUpdateResult(
                accepted=False,
                dry_run=config.dry_run,
                model_snapshot=current_model,
                metrics=metrics,
                redis_write_applied=False,
                postgres_write_applied=False,
                skipped_reason="insufficient_trusted_buckets",
            )

        if config.baseline_update and not baseline_proposal.update_applied:
            metrics = self._build_metrics(
                scan_result=scan_result,
                trusted_bucket_count=len(trusted_buckets),
                skipped_bucket_count=skipped_bucket_count,
                unknown_feature_count=unknown_feature_count,
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

        ad_targets, domain_targets, context_targets = self._build_target_aggregates(
            trusted_buckets=trusted_buckets,
            w0=current_model.w0,
            ad_weights=ad_weights,
            domain_weights=domain_weights,
            context_weights=context_weights,
        )
        updated_ad_weights, ad_max_delta = self._update_family_weights(
            current_weights=ad_weights,
            aggregates=ad_targets,
            config=config,
        )
        updated_domain_weights, domain_max_delta = self._update_family_weights(
            current_weights=domain_weights,
            aggregates=domain_targets,
            config=config,
        )
        updated_context_weights, context_max_delta = self._update_family_weights(
            current_weights=context_weights,
            aggregates=context_targets,
            config=config,
        )

        snapshot_name = self._build_snapshot_name(config.snapshot_name_prefix)
        updated_metrics = SeedModelMetrics(
            baseline_ctr=(
                sigmoid(baseline_proposal.new_w0)
                if config.baseline_update
                else current_model.metrics.baseline_ctr
            ),
            prior_strength=current_model.metrics.prior_strength,
        )
        updated_snapshot = SeedModelSnapshot(
            snapshot_name=snapshot_name,
            w0=baseline_proposal.new_w0,
            weights=SeedWeights(
                w_ad=updated_ad_weights,
                w_dom=updated_domain_weights,
                w_ctx=updated_context_weights,
            ),
            metrics=updated_metrics,
        )
        max_absolute_weight_delta = max(ad_max_delta, domain_max_delta, context_max_delta)
        metrics = self._build_metrics(
            scan_result=scan_result,
            trusted_bucket_count=len(trusted_buckets),
            skipped_bucket_count=skipped_bucket_count,
            unknown_feature_count=unknown_feature_count,
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
        prior_strength = current_model.metrics.prior_strength
        alpha_prior = old_baseline_ctr * prior_strength
        beta_prior = (1.0 - old_baseline_ctr) * prior_strength
        alpha_posterior = alpha_prior + aggregate_clicks
        beta_posterior = beta_prior + aggregate_impressions - aggregate_clicks
        posterior_baseline_ctr = alpha_posterior / (alpha_posterior + beta_posterior)
        variance = beta_variance(alpha_posterior, beta_posterior)
        ci_low, ci_high = clipped_confidence_interval(posterior_baseline_ctr, variance, 1.96)
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

        if aggregate_impressions < config.baseline_min_impressions:
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
                guard_reason="insufficient_baseline_impressions",
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

        target_w0 = safe_logit(posterior_baseline_ctr)
        eta = (
            config.baseline_learning_rate
            * aggregate_impressions
            / (aggregate_impressions + config.baseline_evidence_smoothing)
        )
        raw_delta = eta * (target_w0 - old_w0)
        delta = clip_value(
            raw_delta,
            -config.baseline_max_delta,
            config.baseline_max_delta,
        )
        new_w0 = old_w0 + delta
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

    def _prepare_weight_families(
        self,
        current_model: SeedModelSnapshot,
        trusted_buckets: list[SeedBucketStatistic],
    ) -> tuple[dict[str, float], dict[str, float], dict[str, float], int]:
        """Copy the current weight families and initialize trusted unknown values."""

        ad_weights = dict(current_model.weights.w_ad)
        domain_weights = dict(current_model.weights.w_dom)
        context_weights = dict(current_model.weights.w_ctx)
        unknown_feature_count = 0

        for bucket in trusted_buckets:
            if bucket.ad_category not in ad_weights:
                ad_weights[bucket.ad_category] = 0.0
                unknown_feature_count += 1
            if bucket.publisher_domain not in domain_weights:
                domain_weights[bucket.publisher_domain] = 0.0
                unknown_feature_count += 1
            if bucket.conversation_category not in context_weights:
                context_weights[bucket.conversation_category] = 0.0
                unknown_feature_count += 1

        return ad_weights, domain_weights, context_weights, unknown_feature_count

    def _build_target_aggregates(
        self,
        *,
        trusted_buckets: list[SeedBucketStatistic],
        w0: float,
        ad_weights: dict[str, float],
        domain_weights: dict[str, float],
        context_weights: dict[str, float],
    ) -> tuple[
        dict[str, WeightedTargetAggregate],
        dict[str, WeightedTargetAggregate],
        dict[str, WeightedTargetAggregate],
    ]:
        """Aggregate impression-weighted residual targets for every family value."""

        ad_targets: dict[str, WeightedTargetAggregate] = {}
        domain_targets: dict[str, WeightedTargetAggregate] = {}
        context_targets: dict[str, WeightedTargetAggregate] = {}

        for bucket in trusted_buckets:
            z_target = safe_logit(bucket.ctr)
            impressions = bucket.impressions
            ad_target = (
                z_target
                - w0
                - domain_weights[bucket.publisher_domain]
                - context_weights[bucket.conversation_category]
            )
            domain_target = (
                z_target
                - w0
                - ad_weights[bucket.ad_category]
                - context_weights[bucket.conversation_category]
            )
            context_target = (
                z_target
                - w0
                - ad_weights[bucket.ad_category]
                - domain_weights[bucket.publisher_domain]
            )
            ad_targets.setdefault(bucket.ad_category, WeightedTargetAggregate()).add(
                ad_target,
                impressions,
            )
            domain_targets.setdefault(bucket.publisher_domain, WeightedTargetAggregate()).add(
                domain_target,
                impressions,
            )
            context_targets.setdefault(bucket.conversation_category, WeightedTargetAggregate()).add(
                context_target,
                impressions,
            )

        return ad_targets, domain_targets, context_targets

    def _update_family_weights(
        self,
        *,
        current_weights: dict[str, float],
        aggregates: dict[str, WeightedTargetAggregate],
        config: WeightUpdateRunConfig,
    ) -> tuple[dict[str, float], float]:
        """Apply Task 2 learning, ridge, clipping, and centering to one family."""

        updated_weights = dict(current_weights)
        max_absolute_weight_delta = 0.0

        for feature_value, current_weight in current_weights.items():
            aggregate = aggregates.get(feature_value)
            if aggregate is None or aggregate.impressions == 0:
                continue

            eta = (
                config.learning_rate
                * aggregate.impressions
                / (aggregate.impressions + config.evidence_smoothing)
            )
            raw_delta = eta * (aggregate.weighted_mean - current_weight)
            regularized_delta = raw_delta - config.ridge * current_weight
            delta = clip_value(regularized_delta, -config.max_delta, config.max_delta)
            updated_weights[feature_value] = current_weight + delta
            max_absolute_weight_delta = max(max_absolute_weight_delta, abs(delta))

        return self._center_family(updated_weights), max_absolute_weight_delta

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
        trusted_bucket_count: int,
        skipped_bucket_count: int,
        unknown_feature_count: int,
        max_absolute_weight_delta: float,
        config: WeightUpdateRunConfig,
        baseline_proposal: BaselineUpdateProposal,
    ) -> WeightUpdateRunMetrics:
        """Build the metrics payload recorded for the current run."""

        return WeightUpdateRunMetrics(
            input_bucket_count=scan_result.scanned_key_count,
            valid_bucket_count=scan_result.valid_bucket_count,
            invalid_bucket_count=scan_result.invalid_bucket_count,
            trusted_bucket_count=trusted_bucket_count,
            skipped_bucket_count=skipped_bucket_count,
            unknown_feature_count=unknown_feature_count,
            max_absolute_weight_delta=max_absolute_weight_delta,
            learning_rate=config.learning_rate,
            evidence_smoothing=config.evidence_smoothing,
            ridge=config.ridge,
            max_delta=config.max_delta,
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
