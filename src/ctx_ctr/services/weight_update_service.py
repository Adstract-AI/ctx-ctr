"""Task 2 service for periodic feature-family weight recalibration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedWeights
from ctx_ctr.models.weight_update import (
    RedisBucketScanResult,
    WeightUpdateResult,
    WeightUpdateRunConfig,
    WeightUpdateRunMetrics,
)
from ctx_ctr.services.ctr_math import clip_value, safe_logit

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


class WeightUpdateService:
    """Learn Task 2 feature-family weights from trusted Redis bucket statistics."""

    def __init__(
        self,
        redis_store: WeightUpdateRedisStore,
        postgres_writer: WeightUpdatePostgresWriter | None = None,
    ) -> None:
        self._redis_store = redis_store
        self._postgres_writer = postgres_writer

    def recalibrate(self, config: WeightUpdateRunConfig) -> WeightUpdateResult:
        """Run one Task 2 recalibration pass from Redis to Redis/PostgreSQL."""

        scan_result = self._redis_store.scan_bucket_statistics()
        current_model = self._redis_store.read_current_weights()
        trusted_buckets = [bucket for bucket in scan_result.buckets if bucket.trusted]
        skipped_bucket_count = scan_result.valid_bucket_count - len(trusted_buckets)

        logger.info(
            f"Weight update bucket scan: scanned={scan_result.scanned_key_count}, "
            f"valid={scan_result.valid_bucket_count}, invalid={scan_result.invalid_bucket_count}"
        )
        logger.info(
            f"Weight update trusted buckets: trusted={len(trusted_buckets)}, "
            f"skipped={skipped_bucket_count}"
        )

        ad_weights, domain_weights, context_weights, unknown_feature_count = (
            self._prepare_weight_families(current_model, trusted_buckets)
        )

        if len(trusted_buckets) < config.min_trusted_buckets:
            metrics = self._build_metrics(
                scan_result=scan_result,
                trusted_bucket_count=len(trusted_buckets),
                skipped_bucket_count=skipped_bucket_count,
                unknown_feature_count=unknown_feature_count,
                max_absolute_weight_delta=0.0,
                config=config,
                w0_unchanged=True,
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
        updated_snapshot = SeedModelSnapshot(
            snapshot_name=snapshot_name,
            w0=current_model.w0,
            weights=SeedWeights(
                w_ad=updated_ad_weights,
                w_dom=updated_domain_weights,
                w_ctx=updated_context_weights,
            ),
            metrics=current_model.metrics,
        )
        w0_unchanged = updated_snapshot.w0 == current_model.w0
        max_absolute_weight_delta = max(ad_max_delta, domain_max_delta, context_max_delta)
        metrics = self._build_metrics(
            scan_result=scan_result,
            trusted_bucket_count=len(trusted_buckets),
            skipped_bucket_count=skipped_bucket_count,
            unknown_feature_count=unknown_feature_count,
            max_absolute_weight_delta=max_absolute_weight_delta,
            config=config,
            w0_unchanged=w0_unchanged,
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
        w0_unchanged: bool,
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
            w0_unchanged=w0_unchanged,
        )

    def _build_snapshot_name(self, snapshot_name_prefix: str) -> str:
        """Return the generated snapshot name for the current run."""

        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        return f"{snapshot_name_prefix}_{timestamp}"
