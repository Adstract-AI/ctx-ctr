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
    ExperimentTrafficPhase,
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


class ExperimentSetupRunner(Protocol):
    """Destructive setup operations required by full-system experiments."""

    def clean_topics(self) -> None: ...
    def reset_values(self) -> None: ...
    def seed_values(self) -> None: ...


class ExperimentProcessorManager(Protocol):
    """Child processor lifecycle operations required by full-system experiments."""

    def start_processors(self, definition: ExperimentDefinition, *, run_dir: Path) -> JsonObject: ...
    def assert_healthy(self) -> None: ...
    def stop_processors(self) -> JsonObject: ...


class NoOpExperimentSetupRunner:
    """No-op setup runner used by simple and dry-run experiments."""

    def clean_topics(self) -> None:
        """Skip Kafka topic cleanup."""

    def reset_values(self) -> None:
        """Skip Redis/Postgres reset."""

    def seed_values(self) -> None:
        """Skip deterministic seeding."""


class NoOpExperimentProcessorManager:
    """No-op processor manager used by simple and dry-run experiments."""

    def start_processors(self, definition: ExperimentDefinition, *, run_dir: Path) -> JsonObject:
        """Skip processor startup."""

        return {}

    def assert_healthy(self) -> None:
        """No processors need to be checked."""

    def stop_processors(self) -> JsonObject:
        """Skip processor shutdown."""

        return {}


class ExperimentArtifactWriter:
    """Write experiment artifacts to the local experiments folder."""

    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)

    def run_dir(self, *, experiment_name: str, started_at: datetime) -> Path:
        """Return the deterministic artifact directory for one run."""

        timestamp = started_at.strftime("%Y%m%d_%H%M%S")
        return self._output_dir / f"{timestamp}_{experiment_name}"

    def write(self, result: ExperimentRunResult) -> str:
        """Write JSON and Markdown artifacts and return the JSON artifact path."""

        run_dir = self.run_dir(
            experiment_name=result.experiment_name,
            started_at=result.started_at,
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        json_path = run_dir / "result.json"
        md_path = run_dir / "result.md"
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
        setup_runner: ExperimentSetupRunner | None = None,
        processor_manager: ExperimentProcessorManager | None = None,
    ) -> None:
        self._redis_reader = redis_reader
        self._postgres_store = postgres_store
        self._artifact_writer = artifact_writer
        self._publisher = publisher
        self._setup_runner = setup_runner or NoOpExperimentSetupRunner()
        self._processor_manager = processor_manager or NoOpExperimentProcessorManager()

    def run(self, definition: ExperimentDefinition, *, dry_run: bool) -> ExperimentRunResult:
        """Run one experiment and collect before/after metrics."""

        started_at = datetime.now(tz=UTC)
        run_dir = self._artifact_writer.run_dir(
            experiment_name=definition.experiment_name,
            started_at=started_at,
        )
        run_started_monotonic = time.perf_counter()
        setup_metrics: JsonObject = {}
        processor_start_metrics: JsonObject = {}
        processor_stop_metrics: JsonObject = {}
        phase_results: list[JsonObject] = []
        final_snapshot: ExperimentRuntimeSnapshot | None = None
        before_snapshot: ExperimentRuntimeSnapshot | None = None
        gate_metrics: JsonObject = {"passed": True, "failures": []}
        timing_metrics: JsonObject = {
            "setup": {},
            "processors": {},
            "snapshots": {},
            "validation": {},
            "teardown": {},
        }
        try:
            if not dry_run:
                setup_metrics = self._run_setup(definition)
                timing_metrics["setup"] = setup_metrics.get("timing", {})
            before_snapshot_start = time.perf_counter()
            before_snapshot = self._collect_runtime_snapshot()
            cast(dict[str, JsonValue], timing_metrics["snapshots"])["before_seconds"] = (
                time.perf_counter() - before_snapshot_start
            )
            if definition.traffic.preload_before_processors:
                logger.info("Preloading Kafka traffic before starting processors")
                phase_results = self._run_traffic_phases(definition, dry_run=dry_run)
            if not dry_run:
                processor_start = time.perf_counter()
                processor_start_metrics = self._processor_manager.start_processors(
                    definition,
                    run_dir=run_dir,
                )
                cast(dict[str, JsonValue], timing_metrics["processors"])[
                    "start_seconds"
                ] = time.perf_counter() - processor_start
                self._processor_manager.assert_healthy()
                startup_seconds = self._processor_startup_seconds(definition)
                if startup_seconds > 0:
                    logger.info(f"Waiting {startup_seconds} seconds for processors to start")
                    startup_wait_start = time.perf_counter()
                    time.sleep(startup_seconds)
                    cast(dict[str, JsonValue], timing_metrics["processors"])[
                        "startup_wait_seconds"
                    ] = time.perf_counter() - startup_wait_start
                else:
                    cast(dict[str, JsonValue], timing_metrics["processors"])[
                        "startup_wait_seconds"
                    ] = 0.0
                self._processor_manager.assert_healthy()
            if not definition.traffic.preload_before_processors:
                phase_results = self._run_traffic_phases(definition, dry_run=dry_run)
            if definition.settle_seconds > 0 and not dry_run:
                logger.info(f"Waiting {definition.settle_seconds} seconds for final settle")
                final_settle_start = time.perf_counter()
                time.sleep(definition.settle_seconds)
                timing_metrics["final_settle_seconds"] = (
                    time.perf_counter() - final_settle_start
                )
            else:
                timing_metrics["final_settle_seconds"] = 0.0
            if not dry_run:
                self._processor_manager.assert_healthy()
            final_snapshot_start = time.perf_counter()
            final_snapshot = self._collect_runtime_snapshot()
            cast(dict[str, JsonValue], timing_metrics["snapshots"])["final_seconds"] = (
                time.perf_counter() - final_snapshot_start
            )
            validation_start = time.perf_counter()
            gate_metrics = self._evaluate_success_gates(
                definition=definition,
                before=before_snapshot,
                after=final_snapshot,
                phase_results=phase_results,
                processor_start_metrics=processor_start_metrics,
            )
            cast(dict[str, JsonValue], timing_metrics["validation"])["seconds"] = (
                time.perf_counter() - validation_start
            )
        except ExperimentFailure as error:
            logger.warning(f"Experiment failed before gate evaluation completed: {error}")
            failure_snapshot_start = time.perf_counter()
            final_snapshot = self._collect_runtime_snapshot()
            cast(dict[str, JsonValue], timing_metrics["snapshots"])[
                "failure_final_seconds"
            ] = time.perf_counter() - failure_snapshot_start
            gate_metrics = {
                "passed": False,
                "failures": [str(error)],
            }
        finally:
            if not dry_run:
                teardown_start = time.perf_counter()
                processor_stop_metrics = self._processor_manager.stop_processors()
                timing_metrics["teardown"] = {
                    "processor_stop_seconds": time.perf_counter() - teardown_start
                }

        finished_at = datetime.now(tz=UTC)
        timing_metrics["total_seconds"] = time.perf_counter() - run_started_monotonic
        after_snapshot = final_snapshot or self._collect_runtime_snapshot()
        before_metrics_snapshot = before_snapshot or after_snapshot
        config_payload = self._build_config_payload(definition)
        metrics = self._build_metrics(
            before=before_metrics_snapshot,
            after=after_snapshot,
            phase_results=phase_results,
            processor_stop_metrics=processor_stop_metrics,
            gate_metrics=gate_metrics,
            timing_metrics=timing_metrics,
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

    def _run_setup(self, definition: ExperimentDefinition) -> JsonObject:
        setup = definition.setup
        metrics: JsonObject = {
            "clean_topics": setup.clean_topics,
            "reset_values": setup.reset_values,
            "seed_values": setup.seed_values,
        }
        timings: dict[str, float] = {}
        setup_start = time.perf_counter()
        if setup.clean_topics:
            logger.info("Cleaning Kafka topics for experiment")
            step_start = time.perf_counter()
            self._setup_runner.clean_topics()
            timings["clean_topics_seconds"] = time.perf_counter() - step_start
        if setup.reset_values:
            logger.info("Resetting values for experiment")
            step_start = time.perf_counter()
            self._setup_runner.reset_values()
            timings["reset_values_seconds"] = time.perf_counter() - step_start
        if setup.seed_values:
            logger.info("Seeding values for experiment")
            step_start = time.perf_counter()
            self._setup_runner.seed_values()
            timings["seed_values_seconds"] = time.perf_counter() - step_start
        timings["total_seconds"] = time.perf_counter() - setup_start
        metrics["timing"] = timings
        return metrics

    def _run_traffic_phases(
        self,
        definition: ExperimentDefinition,
        *,
        dry_run: bool,
    ) -> list[JsonObject]:
        phases = self._traffic_phases(definition)
        phase_results: list[JsonObject] = []
        for phase in phases:
            phase_start = time.perf_counter()
            if not dry_run:
                self._processor_manager.assert_healthy()
            produce_start = time.perf_counter()
            producer_result = self._produce_phase(phase, dry_run=dry_run or not definition.traffic.enabled)
            produce_seconds = time.perf_counter() - produce_start
            settle_seconds = 0.0
            if phase.settle_seconds > 0 and not dry_run:
                logger.info(
                    f"Waiting {phase.settle_seconds} seconds after traffic phase {phase.phase_name}"
                )
                settle_start = time.perf_counter()
                time.sleep(phase.settle_seconds)
                settle_seconds = time.perf_counter() - settle_start
            snapshot_start = time.perf_counter()
            snapshot = self._collect_runtime_snapshot()
            snapshot_seconds = time.perf_counter() - snapshot_start
            phase_total_seconds = time.perf_counter() - phase_start
            phase_results.append(
                {
                    "phase_name": phase.phase_name,
                    "producer": cast(
                        JsonObject,
                        producer_result.model_dump(mode="json", exclude={"dry_run"}),
                    ),
                    "snapshot": cast(JsonObject, snapshot.model_dump(mode="json")),
                    "timing": {
                        "produce_seconds": produce_seconds,
                        "settle_seconds": settle_seconds,
                        "snapshot_seconds": snapshot_seconds,
                        "total_seconds": phase_total_seconds,
                        "observed_events_per_second": (
                            producer_result.total_events / produce_seconds
                            if produce_seconds > 0
                            else 0.0
                        ),
                        "observed_impressions_per_second": (
                            producer_result.impressions / produce_seconds
                            if produce_seconds > 0
                            else 0.0
                        ),
                    },
                }
            )
        return phase_results

    def _produce_phase(self, phase: ExperimentTrafficPhase, *, dry_run: bool) -> EventProducerResult:
        run_config = EventProducerRunConfig(
            impressions=phase.impressions,
            events_per_second=phase.events_per_second,
            random_seed=phase.random_seed,
            log_every=phase.log_every,
            also_unified=phase.also_unified,
            dry_run=dry_run,
        )
        logger.info(
            f"Starting experiment traffic phase {phase.phase_name}: "
            f"impressions={phase.impressions}, events_per_second={phase.events_per_second}, "
            f"dry_run={dry_run}"
        )
        return EventSimulatorService(self._publisher).produce(run_config)

    def _traffic_phases(self, definition: ExperimentDefinition) -> list[ExperimentTrafficPhase]:
        traffic = definition.traffic
        if traffic.phases:
            return traffic.phases
        if (
            traffic.impressions is None
            or traffic.events_per_second is None
        ):
            raise ExperimentFailure("traffic_without_phases_is_missing_single_phase_fields")
        return [
            ExperimentTrafficPhase(
                phase_name="default",
                impressions=traffic.impressions,
                events_per_second=traffic.events_per_second,
                random_seed=traffic.random_seed,
                settle_seconds=definition.settle_seconds,
                log_every=traffic.log_every,
                also_unified=traffic.also_unified,
            )
        ]

    def _processor_startup_seconds(self, definition: ExperimentDefinition) -> float:
        processors = [
            definition.processors.realtime_ctr,
            definition.processors.streaming_weight_update,
        ]
        enabled_startups = [
            processor.startup_seconds
            for processor in processors
            if processor.enabled
        ]
        return max(enabled_startups, default=0.0)

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
        return cast(JsonObject, definition.model_dump(mode="json", exclude_none=True))

    def _build_metrics(
        self,
        *,
        before: ExperimentRuntimeSnapshot,
        after: ExperimentRuntimeSnapshot,
        phase_results: list[JsonObject],
        processor_stop_metrics: JsonObject,
        gate_metrics: JsonObject,
        timing_metrics: JsonObject,
    ) -> JsonObject:
        before_payload = cast(JsonObject, before.model_dump(mode="json"))
        after_payload = cast(JsonObject, after.model_dump(mode="json"))
        total_impressions = sum(
            int(cast(JsonObject, phase["producer"])["impressions"])
            for phase in phase_results
        )
        total_clicks = sum(
            int(cast(JsonObject, phase["producer"])["clicks"])
            for phase in phase_results
        )
        total_events = sum(
            int(cast(JsonObject, phase["producer"])["total_events"])
            for phase in phase_results
        )
        metrics: dict[str, JsonValue] = {
            "processor_metrics": self._build_processor_metrics(processor_stop_metrics),
            "traffic": {
                "phases": phase_results,
                "total_impressions": total_impressions,
                "total_clicks": total_clicks,
                "total_events": total_events,
            },
            "producer": {
                "impressions": total_impressions,
                "clicks": total_clicks,
                "total_events": total_events,
            },
            "statistics": {
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
            },
            "success_gates": gate_metrics,
            "timing": timing_metrics,
        }
        return metrics

    def _build_processor_metrics(
        self,
        processor_stop_metrics: JsonObject,
    ) -> JsonObject:
        processor_metrics: JsonObject = {}
        for processor_name, payload in processor_stop_metrics.items():
            if not isinstance(payload, dict):
                continue
            metrics = payload.get("metrics")
            if not isinstance(metrics, dict):
                continue
            raw_records = metrics.get("records")
            records = (
                [record for record in raw_records if isinstance(record, dict)]
                if isinstance(raw_records, list)
                else []
            )
            result: JsonObject = {"records": cast(list[JsonValue], records)}
            if records:
                result["average"] = self._average_processor_records(records)
            processor_metrics[processor_name] = result
        return processor_metrics

    def _average_processor_records(self, records: list[dict[str, object]]) -> JsonObject:
        numeric_values: dict[str, list[float]] = {}
        for record in records:
            for key, value in record.items():
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    continue
                numeric_values.setdefault(key, []).append(float(value))
        return {
            key: sum(values) / len(values)
            for key, values in numeric_values.items()
            if values
        }

    def _evaluate_success_gates(
        self,
        *,
        definition: ExperimentDefinition,
        before: ExperimentRuntimeSnapshot,
        after: ExperimentRuntimeSnapshot,
        phase_results: list[JsonObject],
        processor_start_metrics: JsonObject,
    ) -> JsonObject:
        gates = definition.success_gates
        failures: list[str] = []
        produced_impressions = sum(
            int(cast(JsonObject, phase["producer"])["impressions"])
            for phase in phase_results
        )
        produced_clicks = sum(
            int(cast(JsonObject, phase["producer"])["clicks"])
            for phase in phase_results
        )
        redis_impression_delta = after.redis_total_impressions - before.redis_total_impressions
        redis_click_delta = after.redis_total_clicks - before.redis_total_clicks
        model_snapshot_delta = (
            after.postgres_model_snapshot_count - before.postgres_model_snapshot_count
        )
        if (
            gates.require_redis_impression_delta_match
            and redis_impression_delta != produced_impressions
        ):
            failures.append(
                "redis_impression_delta_mismatch"
            )
        if gates.require_redis_click_delta_match and redis_click_delta != produced_clicks:
            failures.append("redis_click_delta_mismatch")
        if gates.require_model_snapshot_created and model_snapshot_delta < 1:
            failures.append("model_snapshot_not_created")
        if after.redis_invalid_bucket_count > gates.max_invalid_redis_buckets:
            failures.append("invalid_redis_bucket_count_too_high")
        if gates.require_processor_health:
            unhealthy_processors = [
                name
                for name, payload in processor_start_metrics.items()
                if isinstance(payload, dict) and payload.get("early_exit") is True
            ]
            if unhealthy_processors:
                failures.append("processor_exited_early")
        return {
            "passed": not failures,
            "failures": failures,
            "produced_impressions": produced_impressions,
            "produced_clicks": produced_clicks,
            "redis_impression_delta": redis_impression_delta,
            "redis_click_delta": redis_click_delta,
            "model_snapshot_delta": model_snapshot_delta,
            "max_invalid_redis_buckets": gates.max_invalid_redis_buckets,
        }


class ExperimentFailure(RuntimeError):
    """Raised when a strict experiment success gate fails."""
