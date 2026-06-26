"""Service for running measurable CTR experiments."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.experiment import (
    ExperimentDefinition,
    ExperimentRunResult,
    ExperimentRuntimeSnapshot,
    JsonObject,
    JsonValue,
)
from ctx_ctr.models.weight_update import RedisBucketScanResult
from ctx_ctr.services.event_simulator import (
    EventProducerResult,
    EventProducerRunConfig,
    EventPublisher,
    EventSimulatorService,
)

logger = get_logger(__name__)


class ExperimentRedisReader(Protocol):
    """Redis metrics required by the experiment service."""

    def scan_bucket_statistics(self) -> RedisBucketScanResult: ...
    def read_current_weights(self) -> object: ...


class ExperimentPostgresStore(Protocol):
    """PostgreSQL metrics and persistence required by the experiment service."""

    def count_model_snapshots(self) -> int: ...
    def count_experiment_results(self) -> int: ...
    def insert_experiment_result(
        self,
        *,
        experiment_name: str,
        config: JsonObject,
        metrics: JsonObject,
        artifact_uri: str | None,
    ) -> int: ...


class ExperimentArtifactWriter:
    """Write experiment artifacts to the local experiments folder."""

    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)

    def write(self, result: ExperimentRunResult) -> str:
        """Write JSON and Markdown artifacts and return the JSON artifact path."""

        self._output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = result.started_at.strftime("%Y%m%d_%H%M%S")
        stem = f"{timestamp}_{result.experiment_name}"
        json_path = self._output_dir / f"{stem}.json"
        md_path = self._output_dir / f"{stem}.md"
        payload = result.model_dump(mode="json")
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        md_path.write_text(self._build_markdown(payload), encoding="utf-8")
        return str(json_path)

    def _build_markdown(self, payload: dict[str, object]) -> str:
        metrics = cast(dict[str, object], payload["metrics"])
        lines = [
            f"# {payload['experiment_name']}",
            "",
            f"- started_at: `{payload['started_at']}`",
            f"- finished_at: `{payload['finished_at']}`",
            f"- duration_seconds: `{payload['duration_seconds']}`",
            f"- dry_run: `{payload['dry_run']}`",
            "",
            "## Metrics",
            "",
        ]
        for key, value in metrics.items():
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")
        return "\n".join(lines)


class ExperimentService:
    """Run one configured experiment and persist its result."""

    def __init__(
        self,
        *,
        redis_reader: ExperimentRedisReader,
        postgres_store: ExperimentPostgresStore,
        artifact_writer: ExperimentArtifactWriter,
        publisher: EventPublisher,
    ) -> None:
        self._redis_reader = redis_reader
        self._postgres_store = postgres_store
        self._artifact_writer = artifact_writer
        self._publisher = publisher

    def run(self, definition: ExperimentDefinition, *, dry_run: bool) -> ExperimentRunResult:
        """Run one experiment and collect before/after metrics."""

        started_at = datetime.now(tz=UTC)
        before_snapshot = self._collect_runtime_snapshot()
        producer_result = self._produce_traffic(definition, dry_run=dry_run)
        if definition.settle_seconds > 0 and not dry_run:
            logger.info(f"Waiting {definition.settle_seconds} seconds for processors to settle")
            time.sleep(definition.settle_seconds)
        after_snapshot = self._collect_runtime_snapshot()
        finished_at = datetime.now(tz=UTC)
        config_payload = self._build_config_payload(definition)
        metrics = self._build_metrics(
            before=before_snapshot,
            after=after_snapshot,
            producer_result=producer_result,
        )
        result = ExperimentRunResult(
            experiment_name=definition.experiment_name,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            dry_run=dry_run,
            config=config_payload,
            metrics=metrics,
        )
        artifact_uri = self._artifact_writer.write(result)
        postgres_experiment_id = None
        if not dry_run:
            postgres_experiment_id = self._postgres_store.insert_experiment_result(
                experiment_name=definition.experiment_name,
                config=config_payload,
                metrics=metrics,
                artifact_uri=artifact_uri,
            )
        final_result = result.model_copy(
            update={
                "artifact_uri": artifact_uri,
                "postgres_experiment_id": postgres_experiment_id,
            }
        )
        self._artifact_writer.write(final_result)
        return final_result

    def _produce_traffic(
        self,
        definition: ExperimentDefinition,
        *,
        dry_run: bool,
    ) -> EventProducerResult:
        traffic = definition.traffic
        run_config = EventProducerRunConfig(
            impressions=traffic.impressions,
            events_per_second=traffic.events_per_second,
            random_seed=traffic.random_seed,
            log_every=traffic.log_every,
            also_unified=traffic.also_unified,
            dry_run=dry_run or not traffic.enabled,
        )
        if not traffic.enabled:
            logger.info("Experiment traffic generation is disabled")
        return EventSimulatorService(self._publisher).produce(run_config)

    def _collect_runtime_snapshot(self) -> ExperimentRuntimeSnapshot:
        scan_result = self._redis_reader.scan_bucket_statistics()
        buckets = scan_result.buckets
        total_impressions = sum(bucket.impressions for bucket in buckets)
        total_clicks = sum(bucket.clicks for bucket in buckets)
        trusted_count = sum(1 for bucket in buckets if bucket.trusted)
        ci_widths = [bucket.ci_high - bucket.ci_low for bucket in buckets]
        model_snapshot_name = None
        model_baseline_ctr = None
        try:
            current_model = self._redis_reader.read_current_weights()
            model_snapshot_name = str(getattr(current_model, "snapshot_name"))
            model_baseline_ctr = float(getattr(getattr(current_model, "metrics"), "baseline_ctr"))
        except Exception as error:
            logger.warning(f"Could not read current model while collecting experiment metrics: {error}")
        return ExperimentRuntimeSnapshot(
            redis_bucket_keys_scanned=scan_result.scanned_key_count,
            redis_valid_bucket_count=scan_result.valid_bucket_count,
            redis_invalid_bucket_count=scan_result.invalid_bucket_count,
            redis_total_impressions=total_impressions,
            redis_total_clicks=total_clicks,
            redis_trusted_bucket_count=trusted_count,
            redis_average_ci_width=(sum(ci_widths) / len(ci_widths) if ci_widths else 0.0),
            redis_max_ci_width=max(ci_widths, default=0.0),
            current_model_snapshot_name=model_snapshot_name,
            current_model_baseline_ctr=model_baseline_ctr,
            postgres_model_snapshot_count=self._postgres_store.count_model_snapshots(),
            postgres_experiment_result_count=self._postgres_store.count_experiment_results(),
        )

    def _build_config_payload(self, definition: ExperimentDefinition) -> JsonObject:
        return cast(JsonObject, definition.model_dump(mode="json"))

    def _build_metrics(
        self,
        *,
        before: ExperimentRuntimeSnapshot,
        after: ExperimentRuntimeSnapshot,
        producer_result: EventProducerResult,
    ) -> JsonObject:
        before_payload = cast(JsonObject, before.model_dump(mode="json"))
        after_payload = cast(JsonObject, after.model_dump(mode="json"))
        metrics: dict[str, JsonValue] = {
            "producer": cast(JsonObject, producer_result.model_dump(mode="json")),
            "before": before_payload,
            "after": after_payload,
            "delta": {
                "redis_total_impressions": (
                    after.redis_total_impressions - before.redis_total_impressions
                ),
                "redis_total_clicks": after.redis_total_clicks - before.redis_total_clicks,
                "redis_valid_bucket_count": (
                    after.redis_valid_bucket_count - before.redis_valid_bucket_count
                ),
                "redis_trusted_bucket_count": (
                    after.redis_trusted_bucket_count - before.redis_trusted_bucket_count
                ),
                "postgres_model_snapshot_count": (
                    after.postgres_model_snapshot_count - before.postgres_model_snapshot_count
                ),
                "postgres_experiment_result_count": (
                    after.postgres_experiment_result_count
                    - before.postgres_experiment_result_count
                ),
            },
        }
        return metrics
