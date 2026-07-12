"""Command entrypoint for Flink-native streaming weight updates."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ctx_ctr.adapters.redis_runtime import RedisRuntimeAdapter
from ctx_ctr.env_variables import (
    KAFKA_BOOTSTRAP_SERVERS,
    LOG_COLOR,
    LOG_LEVEL,
    POSTGRES_DSN,
    REDIS_URL,
)
from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import format_summary_box, print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.job_configs import RunStreamingWeightUpdateJobConfig
from ctx_ctr.models.seed import SeedModelSnapshot
from ctx_ctr.models.streaming_weight_update import StreamingWeightEvidenceState
from ctx_ctr.models.weight_update import RedisBucketScanResult, WeightUpdateRunConfig
from ctx_ctr.services.streaming_weight_update_service import StreamingWeightUpdateEvidenceBuilder
from ctx_ctr.services.weight_update_service import WeightUpdateService

logger = get_logger("ctx_ctr.jobs.run_streaming_weight_update")
DEFAULT_CONFIG_PATH = "configs/run_streaming_weight_update.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
GLOBAL_WEIGHT_UPDATE_KEY = "global-weight-update"


class StreamingWeightRedisStore:
    """Redis store facade backed by current Flink evidence at timer time."""

    def __init__(
        self,
        redis_adapter: RedisRuntimeAdapter,
        *,
        current_model: SeedModelSnapshot,
        scan_result: RedisBucketScanResult,
    ) -> None:
        self._redis_adapter = redis_adapter
        self._current_model = current_model
        self._scan_result = scan_result

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        """Return the Flink-built evidence snapshot instead of scanning Redis buckets."""

        return self._scan_result

    def read_current_weights(self) -> SeedModelSnapshot:
        """Return the current in-memory model for this timer."""

        return self._current_model

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        """Persist the accepted model to Redis."""

        self._redis_adapter.write_current_weights(snapshot)


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and start the streaming weight-update Flink job."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    file_config = load_job_config(args.config, RunStreamingWeightUpdateJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    logger.info(
        "Configured streaming weight update job: "
        f"impression_topic={config.impression_topic}, click_topic={config.click_topic}, "
        f"consumer_group={config.consumer_group}, parallelism={config.parallelism}, "
        f"checkpoint_interval_ms={config.checkpoint_interval_ms}, "
        f"interval_seconds={config.interval_seconds}, dry_run={config.dry_run}, "
        f"config={args.config}"
    )

    try:
        run_flink_streaming_weight_update_job(config)
    except KeyboardInterrupt:
        print_success(
            "Streaming weight update job stopped",
            [
                "status: interrupted by user",
                f"impression topic: {config.impression_topic}",
                f"click topic: {config.click_topic}",
            ],
        )
    except Exception as error:
        print_failure("Streaming weight update job", error)
        raise


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the streaming weight-update job."""

    parser = argparse.ArgumentParser(description="Run Flink-native streaming weight updates.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--impression-topic", default=None)
    parser.add_argument("--click-topic", default=None)
    parser.add_argument("--consumer-group", default=None)
    parser.add_argument("--parallelism", type=int, default=None)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=None)
    parser.add_argument("--kafka-connector-jar", default=None)
    parser.add_argument("--interval-seconds", type=int, default=None)
    parser.add_argument("--learning-rate", type=float, default=None)
    parser.add_argument("--evidence-smoothing", type=float, default=None)
    parser.add_argument("--ridge", type=float, default=None)
    parser.add_argument("--max-delta", type=float, default=None)
    parser.add_argument("--min-feature-impressions", type=int, default=None)
    parser.add_argument("--max-feature-ci-width", type=float, default=None)
    parser.add_argument("--snapshot-name-prefix", default=None)
    parser.add_argument(
        "--baseline-update",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="enable or disable global baseline w0 updates",
    )
    parser.add_argument("--baseline-max-ci-width", type=float, default=None)
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="compute updates without writing Redis or PostgreSQL",
    )
    return parser


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "impression_topic",
        "click_topic",
        "consumer_group",
        "parallelism",
        "checkpoint_interval_ms",
        "kafka_connector_jar",
        "interval_seconds",
        "learning_rate",
        "evidence_smoothing",
        "ridge",
        "max_delta",
        "min_feature_impressions",
        "max_feature_ci_width",
        "snapshot_name_prefix",
        "baseline_update",
        "baseline_max_ci_width",
        "dry_run",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


def run_flink_streaming_weight_update_job(config: RunStreamingWeightUpdateJobConfig) -> None:
    """Build and execute the local PyFlink streaming weight-update topology."""

    from pyflink.common import Types, WatermarkStrategy  # type: ignore[import-untyped]
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import-untyped]
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-untyped]
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import-untyped]
        KafkaOffsetsInitializer,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import-untyped]
    from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import-untyped]

    class StreamingWeightUpdateProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Maintain cumulative evidence and update weights on processing-time timers."""

        def __init__(self, run_config: WeightUpdateRunConfig, interval_ms: int) -> None:
            self._run_config = run_config
            self._interval_ms = interval_ms
            self._evidence_builder = StreamingWeightUpdateEvidenceBuilder()
            self._redis_adapter: RedisRuntimeAdapter | None = None
            self._postgres_context: Any | None = None
            self._postgres_writer: Any | None = None
            self._evidence_state: Any | None = None
            self._model_state: Any | None = None
            self._startup_model: SeedModelSnapshot | None = None

        def open(self, runtime_context: Any) -> None:
            """Open runtime adapters and state handles."""

            self._redis_adapter = RedisRuntimeAdapter(REDIS_URL)
            self._evidence_state = runtime_context.get_state(
                ValueStateDescriptor("streaming_weight_evidence", Types.STRING())
            )
            self._model_state = runtime_context.get_state(
                ValueStateDescriptor("streaming_weight_current_model", Types.STRING())
            )
            if not self._run_config.dry_run:
                from ctx_ctr.adapters.postgres_runtime import PostgresRuntimeAdapter

                self._postgres_context = PostgresRuntimeAdapter(POSTGRES_DSN)
                self._postgres_writer = self._postgres_context.__enter__()
            current_model = self._redis_adapter.read_current_weights()
            self._startup_model = current_model
            logger.info(
                "Loaded current model for streaming weight update: "
                f"snapshot_name={current_model.snapshot_name}"
            )

        def process_element(self, value: str, ctx: Any) -> Any:
            """Accumulate one Kafka event into Flink state."""

            self._ensure_current_model_state()
            evidence_state = self._load_evidence_state()
            try:
                event = CtrEvent.model_validate_json(value)
            except ValidationError:
                evidence_state = self._evidence_builder.mark_invalid_event(evidence_state)
                self._store_evidence_state(evidence_state)
                return

            evidence_state = self._evidence_builder.apply_event(evidence_state, event)
            if evidence_state.next_timer_ms is None:
                next_timer_ms = ctx.timer_service().current_processing_time() + self._interval_ms
                ctx.timer_service().register_processing_time_timer(next_timer_ms)
                evidence_state = evidence_state.model_copy(update={"next_timer_ms": next_timer_ms})
                logger.info(
                    f"Registered first streaming weight-update timer at {next_timer_ms}"
                )
            self._store_evidence_state(evidence_state)

        def on_timer(self, timestamp: int, ctx: Any) -> Any:
            """Run one weight update and schedule the next processing-time timer."""

            evidence_state = self._load_evidence_state()
            current_model = self._load_current_model()
            evidence_snapshot = self._evidence_builder.build_snapshot(
                state=evidence_state,
                current_model=current_model,
            )
            scan_result = RedisBucketScanResult(
                scanned_key_count=evidence_snapshot.scanned_key_count,
                valid_bucket_count=evidence_snapshot.valid_bucket_count,
                invalid_bucket_count=evidence_snapshot.invalid_bucket_count,
                buckets=evidence_snapshot.buckets,
            )
            redis_adapter = self._require_redis_adapter()
            redis_store = StreamingWeightRedisStore(
                redis_adapter,
                current_model=current_model,
                scan_result=scan_result,
            )
            service = WeightUpdateService(
                redis_store=redis_store,
                postgres_writer=self._postgres_writer,
            )
            result = service.recalibrate(self._run_config)
            if result.accepted:
                self._require_model_state().update(result.model_snapshot.model_dump_json())

            next_timer_ms = timestamp + self._interval_ms
            evidence_state = evidence_state.model_copy(update={"next_timer_ms": next_timer_ms})
            self._store_evidence_state(evidence_state)
            ctx.timer_service().register_processing_time_timer(next_timer_ms)
            summary = format_summary_box(
                heading="STREAMING WEIGHT UPDATE",
                title="Streaming Weight Update Timer",
                status="ACCEPTED" if result.accepted else "SKIPPED",
                lines=[
                    f"dry run: {result.dry_run}",
                    f"processed events: {evidence_state.processed_events}",
                    f"invalid events: {evidence_state.invalid_events}",
                    f"triplets: {len(evidence_state.triplets)}",
                    f"valid buckets: {result.metrics.valid_bucket_count}",
                    f"invalid buckets: {result.metrics.invalid_bucket_count}",
                    f"updated features: {result.metrics.updated_feature_bucket_count}",
                    f"baseline applied: {result.metrics.baseline_update_applied}",
                    f"baseline ci width: {result.metrics.baseline_ci_width:.6f}",
                    f"baseline guard: {result.metrics.baseline_guard_reason}",
                    f"redis write: {result.redis_write_applied}",
                    f"postgres write: {result.postgres_write_applied}",
                    f"snapshot: {result.snapshot_name or 'not generated'}",
                    f"skip reason: {result.skipped_reason or 'none'}",
                ],
            )
            if result.accepted:
                logger.info("\n" + summary)
            else:
                logger.warning("\n" + summary)
            yield from ()

        def close(self) -> None:
            """Close runtime resources."""

            if self._postgres_context is not None:
                self._postgres_context.__exit__(None, None, None)

        def _load_evidence_state(self) -> StreamingWeightEvidenceState:
            state = self._require_evidence_state()
            payload = state.value()
            if payload is None:
                return StreamingWeightEvidenceState()
            return StreamingWeightEvidenceState.model_validate_json(payload)

        def _store_evidence_state(self, evidence_state: StreamingWeightEvidenceState) -> None:
            self._require_evidence_state().update(evidence_state.model_dump_json())

        def _load_current_model(self) -> SeedModelSnapshot:
            payload = self._require_model_state().value()
            if payload is None and self._startup_model is not None:
                self._require_model_state().update(self._startup_model.model_dump_json())
                return self._startup_model
            if payload is None:
                current_model = self._require_redis_adapter().read_current_weights()
                self._require_model_state().update(current_model.model_dump_json())
                return current_model
            return SeedModelSnapshot.model_validate_json(payload)

        def _ensure_current_model_state(self) -> None:
            if self._require_model_state().value() is None:
                current_model = self._startup_model or self._require_redis_adapter().read_current_weights()
                self._require_model_state().update(current_model.model_dump_json())

        def _require_redis_adapter(self) -> RedisRuntimeAdapter:
            if self._redis_adapter is None:
                raise WeightUpdateError("Redis adapter is not initialized")
            return self._redis_adapter

        def _require_evidence_state(self) -> Any:
            if self._evidence_state is None:
                raise WeightUpdateError("Flink evidence state is not initialized")
            return self._evidence_state

        def _require_model_state(self) -> Any:
            if self._model_state is None:
                raise WeightUpdateError("Flink model state is not initialized")
            return self._model_state

    run_config = WeightUpdateRunConfig(
        learning_rate=config.learning_rate,
        evidence_smoothing=config.evidence_smoothing,
        ridge=config.ridge,
        max_delta=config.max_delta,
        min_feature_impressions=config.min_feature_impressions,
        max_feature_ci_width=config.max_feature_ci_width,
        snapshot_name_prefix=config.snapshot_name_prefix,
        baseline_update=config.baseline_update,
        baseline_max_ci_width=config.baseline_max_ci_width,
        dry_run=config.dry_run,
    )
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)
    kafka_connector_jar = _resolve_project_path(config.kafka_connector_jar)
    if not kafka_connector_jar.is_file():
        raise FileNotFoundError(
            "Flink Kafka connector jar was not found at "
            f"{kafka_connector_jar}. Download it into the project jars folder "
            "or override --kafka-connector-jar."
        )
    env.add_jars(kafka_connector_jar.as_uri())
    if config.checkpoint_interval_ms > 0:
        env.enable_checkpointing(config.checkpoint_interval_ms)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_group_id(config.consumer_group)
        .set_topics(config.impression_topic, config.click_topic)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    summaries = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "weight-update-events")
        .key_by(lambda _event: GLOBAL_WEIGHT_UPDATE_KEY, key_type=Types.STRING())
        .process(
            StreamingWeightUpdateProcessFunction(
                run_config=run_config,
                interval_ms=config.interval_seconds * 1000,
            ),
            output_type=Types.STRING(),
        )
    )
    summaries.print()
    with _suppress_py4j_keyboard_interrupt_log():
        env.execute("ctx-ctr-streaming-weight-update")


def _resolve_project_path(path_value: str) -> Path:
    """Resolve absolute paths as-is and relative paths from the project root."""

    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


@contextmanager
def _suppress_py4j_keyboard_interrupt_log() -> Iterator[None]:
    """Hide Py4J's noisy root traceback for user-requested Ctrl+C shutdowns."""

    class Py4jKeyboardInterruptFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return record.getMessage() != "KeyboardInterrupt while sending command."

    root_logger = logging.getLogger()
    log_filter = Py4jKeyboardInterruptFilter()
    root_logger.addFilter(log_filter)
    try:
        yield
    finally:
        root_logger.removeFilter(log_filter)


if __name__ == "__main__":
    main()
