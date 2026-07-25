"""Command entrypoint for running realtime CTR updates with PyFlink."""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pydantic import ValidationError

from ctx_ctr.adapters.redis_ctr_state import RedisCtrStateAdapter
from ctx_ctr.env_variables import KAFKA_BOOTSTRAP_SERVERS, LOG_COLOR, LOG_LEVEL, REDIS_URL
from ctx_ctr.exceptions import CtrStateError
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.ctr_state import CtrBucketStatistic, DeadLetterPayload
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.job_configs import RunRealtimeCtrJobConfig
from ctx_ctr.services.realtime_ctr import (
    CtrTrustThresholds,
    RealtimeCtrUpdateService,
    RedisFlushAction,
    event_bucket_key,
    redis_flush_action,
)

logger = get_logger("ctx_ctr.jobs.run_realtime_ctr")

INVALID_BUCKET_KEY = "__invalid_event__"
DEFAULT_CONFIG_PATH = "configs/run_realtime_ctr.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
EARLY_REDIS_FLUSH_DELAY_MS = 1


def main() -> None:
    """Parse CLI arguments and start the realtime CTR Flink job."""

    parser = argparse.ArgumentParser(description="Run realtime CTR updates with PyFlink.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--impression-topic", default=None)
    parser.add_argument("--click-topic", default=None)
    parser.add_argument("--dead-letter-topic", default=None)
    parser.add_argument("--consumer-group", default=None)
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default=None,
        help="Kafka offsets used when the consumer group has no committed offsets",
    )
    parser.add_argument("--parallelism", type=int, default=None)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=None)
    parser.add_argument(
        "--kafka-connector-jar",
        default=None,
        help="optional path to the Flink Kafka connector jar for local PyFlink",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=None,
        help="log progress every N valid events; use 0 to disable progress logs",
    )
    parser.add_argument(
        "--metrics-flush-interval-ms",
        type=int,
        default=None,
        help="flush partial processor metrics after this interval; use 0 to disable",
    )
    parser.add_argument(
        "--redis-flush-mode",
        choices=("periodic", "per_event"),
        default=None,
        help="write every accepted update immediately or flush dirty buckets periodically",
    )
    parser.add_argument(
        "--redis-flush-interval-ms",
        type=int,
        default=None,
        help="flush dirty bucket state to Redis after this processing-time interval",
    )
    parser.add_argument(
        "--redis-flush-max-updates",
        type=int,
        default=None,
        help="flush a dirty bucket after this many updates even before its timer",
    )
    parser.add_argument("--trust-z-score", type=float, default=None)
    parser.add_argument("--trust-min-impressions", type=int, default=None)
    parser.add_argument("--trust-max-variance", type=float, default=None)
    parser.add_argument("--trust-max-ci-width", type=float, default=None)
    args = parser.parse_args()
    file_config = load_job_config(args.config, RunRealtimeCtrJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))

    configure_logging(LOG_LEVEL, LOG_COLOR)
    logger.info(
        "Configured realtime CTR job: "
        f"impression_topic={config.impression_topic}, click_topic={config.click_topic}, "
        f"dead_letter_topic={config.dead_letter_topic}, consumer_group={config.consumer_group}, "
        f"starting_offsets={config.starting_offsets}, "
        f"parallelism={config.parallelism}, checkpoint_interval_ms={config.checkpoint_interval_ms}, "
        f"trust_min_impressions={config.trust_min_impressions}, "
        f"trust_max_variance={config.trust_max_variance}, "
        f"trust_max_ci_width={config.trust_max_ci_width}, "
        f"metrics_flush_interval_ms={config.metrics_flush_interval_ms}, "
        f"redis_flush_mode={config.redis_flush_mode}, "
        f"redis_flush_interval_ms={config.redis_flush_interval_ms}, "
        f"redis_flush_max_updates={config.redis_flush_max_updates}, "
        f"config={args.config}"
    )

    try:
        run_flink_realtime_ctr_job(config)
    except KeyboardInterrupt:
        print_success(
            "Realtime CTR job stopped",
            [
                "status: interrupted by user",
                f"impression topic: {config.impression_topic}",
                f"click topic: {config.click_topic}",
                f"dead-letter topic: {config.dead_letter_topic}",
            ],
        )
    except Exception as error:
        print_failure("Realtime CTR job", error)
        raise


def run_flink_realtime_ctr_job(config: RunRealtimeCtrJobConfig) -> None:
    """Build and execute the local PyFlink realtime CTR topology."""

    from pyflink.common import Types, WatermarkStrategy  # type: ignore[import-untyped]
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import-untyped]
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-untyped]
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import-untyped]
        DeliveryGuarantee,
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import-untyped]
    from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import-untyped]

    class RedisCtrProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Process keyed CTR events and persist updated buckets to Redis."""

        def __init__(
            self,
            redis_url: str,
            log_every: int,
            metrics_flush_interval_ms: int,
            redis_flush_mode: str,
            redis_flush_interval_ms: int,
            redis_flush_max_updates: int,
        ) -> None:
            self._redis_url = redis_url
            self._log_every = log_every
            self._metrics_flush_interval_ms = metrics_flush_interval_ms
            self._redis_flush_mode = redis_flush_mode
            self._redis_flush_interval_ms = redis_flush_interval_ms
            self._redis_flush_max_updates = redis_flush_max_updates
            self._trust_thresholds = CtrTrustThresholds(
                z_score=config.trust_z_score,
                min_impressions=config.trust_min_impressions,
                max_variance=config.trust_max_variance,
                max_ci_width=config.trust_max_ci_width,
            )
            self._adapter: RedisCtrStateAdapter | None = None
            self._service: RealtimeCtrUpdateService | None = None
            self._bucket_state: Any | None = None
            self._dirty_update_count_state: Any | None = None
            self._redis_flush_timer_state: Any | None = None
            self._valid_events = 0
            self._processed_events = 0
            self._impressions = 0
            self._clicks = 0
            self._dead_letters = 0
            self._redis_flushes = 0
            self._redis_updates_flushed = 0
            self._redis_updates_coalesced = 0
            self._started_monotonic = time.perf_counter()
            self._last_log_monotonic = self._started_monotonic
            self._last_event_monotonic = self._started_monotonic
            self._last_log_processed_events = 0
            self._metrics_timer_timestamp: int | None = None
            self._subtask_index = 0

        def open(self, runtime_context: Any) -> None:
            """Initialize Redis-backed model state and keyed Flink state."""

            self._adapter = RedisCtrStateAdapter(self._redis_url)
            self._subtask_index = runtime_context.get_index_of_this_subtask()
            model = self._adapter.read_current_model()
            self._service = RealtimeCtrUpdateService(model, self._trust_thresholds)
            self._bucket_state = runtime_context.get_state(
                ValueStateDescriptor("bucket_state", Types.STRING())
            )
            self._dirty_update_count_state = runtime_context.get_state(
                ValueStateDescriptor("redis_dirty_update_count", Types.INT())
            )
            self._redis_flush_timer_state = runtime_context.get_state(
                ValueStateDescriptor("redis_flush_timer", Types.LONG())
            )
            logger.info(
                f"Loaded current CTR model from Redis: snapshot_name={model.snapshot_name}"
            )

        def process_element(
            self,
            value: str,
            runtime_context: Any,
        ) -> Any:
            """Apply one raw Kafka record to CTR state or emit a dead-letter payload."""

            self._last_event_monotonic = time.perf_counter()
            self._ensure_metrics_timer(runtime_context)
            try:
                event = CtrEvent.model_validate_json(value)
            except ValidationError:
                self._processed_events += 1
                self._dead_letters += 1
                self._log_metrics()
                yield _dead_letter_json("invalid_event_payload", {"raw": value})
                return

            service = self._require_service()
            adapter = self._require_adapter()
            state = self._require_bucket_state()
            bucket = self._load_bucket_state(event, adapter, state)
            result = service.apply_event(event, bucket)

            if result.dead_letter is not None:
                self._processed_events += 1
                self._dead_letters += 1
                self._log_metrics()
                yield result.dead_letter.model_dump_json()
                return

            if result.bucket is None:
                raise CtrStateError("Realtime CTR update returned no bucket for valid event")

            state.update(result.bucket.model_dump_json())
            if self._redis_flush_mode == "per_event":
                adapter.write_bucket_statistic(result.bucket)
                self._redis_flushes += 1
                self._redis_updates_flushed += 1
            else:
                self._mark_bucket_dirty(runtime_context)
            self._processed_events += 1
            self._valid_events += 1
            if event.event_type == "impression":
                self._impressions += 1
            elif event.event_type == "click":
                self._clicks += 1
            self._log_metrics()

        def on_timer(self, timestamp: int, runtime_context: Any) -> Iterator[str]:
            """Handle keyed Redis flush timers and the operator metrics timer."""

            redis_timer_state = self._require_redis_flush_timer_state()
            if redis_timer_state.value() == timestamp:
                bucket_payload = self._require_bucket_state().value()
                if bucket_payload is None:
                    raise CtrStateError("Redis flush timer fired without bucket state")
                self._flush_bucket_to_redis(
                    CtrBucketStatistic.model_validate_json(bucket_payload),
                    runtime_context,
                )

            if timestamp == self._metrics_timer_timestamp:
                self._metrics_timer_timestamp = None
                has_unlogged_events = (
                    self._processed_events != self._last_log_processed_events
                )
                idle_seconds = time.perf_counter() - self._last_event_monotonic
                flush_interval_seconds = self._metrics_flush_interval_ms / 1000
                if has_unlogged_events and idle_seconds >= flush_interval_seconds:
                    self._log_metrics(force=True)
                elif has_unlogged_events:
                    self._ensure_metrics_timer(runtime_context)
            yield from ()

        def close(self) -> None:
            """Close Redis resources."""

            self._log_metrics(force=True)
            if self._adapter is not None:
                self._adapter.close()

        def _load_bucket_state(
            self,
            event: CtrEvent,
            adapter: RedisCtrStateAdapter,
            state: Any,
        ) -> CtrBucketStatistic | None:
            state_payload = state.value()
            if state_payload is not None:
                return CtrBucketStatistic.model_validate_json(state_payload)
            return adapter.read_bucket_statistic(event_bucket_key(event))

        def _require_adapter(self) -> RedisCtrStateAdapter:
            if self._adapter is None:
                raise CtrStateError("Redis adapter is not initialized")
            return self._adapter

        def _require_service(self) -> RealtimeCtrUpdateService:
            if self._service is None:
                raise CtrStateError("Realtime CTR service is not initialized")
            return self._service

        def _require_bucket_state(self) -> Any:
            if self._bucket_state is None:
                raise CtrStateError("Flink bucket state is not initialized")
            return self._bucket_state

        def _require_dirty_update_count_state(self) -> Any:
            if self._dirty_update_count_state is None:
                raise CtrStateError("Redis dirty update state is not initialized")
            return self._dirty_update_count_state

        def _require_redis_flush_timer_state(self) -> Any:
            if self._redis_flush_timer_state is None:
                raise CtrStateError("Redis flush timer state is not initialized")
            return self._redis_flush_timer_state

        def _mark_bucket_dirty(
            self,
            runtime_context: Any,
        ) -> None:
            dirty_state = self._require_dirty_update_count_state()
            dirty_updates = int(dirty_state.value() or 0) + 1
            dirty_state.update(dirty_updates)
            timer_state = self._require_redis_flush_timer_state()
            action = redis_flush_action(
                dirty_updates=dirty_updates,
                max_updates=self._redis_flush_max_updates,
                has_timer=timer_state.value() is not None,
            )
            if action is RedisFlushAction.KEEP_TIMER:
                return
            if action is RedisFlushAction.ACCELERATE_TIMER:
                self._schedule_redis_flush(
                    runtime_context,
                    delay_ms=EARLY_REDIS_FLUSH_DELAY_MS,
                    replace_existing=True,
                )
                return

            self._schedule_redis_flush(
                runtime_context,
                delay_ms=self._redis_flush_interval_ms,
                replace_existing=False,
            )

        def _schedule_redis_flush(
            self,
            runtime_context: Any,
            *,
            delay_ms: int,
            replace_existing: bool,
        ) -> None:
            timer_state = self._require_redis_flush_timer_state()
            existing_timestamp = timer_state.value()
            if existing_timestamp is not None and not replace_existing:
                return

            timer_service = runtime_context.timer_service()
            if existing_timestamp is not None:
                timer_service.delete_processing_time_timer(existing_timestamp)
            timestamp = timer_service.current_processing_time() + delay_ms
            timer_service.register_processing_time_timer(timestamp)
            timer_state.update(timestamp)

        def _flush_bucket_to_redis(
            self,
            bucket: CtrBucketStatistic,
            runtime_context: Any,
        ) -> None:
            dirty_state = self._require_dirty_update_count_state()
            dirty_updates = int(dirty_state.value() or 0)
            if dirty_updates <= 0:
                return

            self._require_adapter().write_bucket_statistic(bucket)
            timer_state = self._require_redis_flush_timer_state()
            timer_timestamp = timer_state.value()
            if timer_timestamp is not None:
                runtime_context.timer_service().delete_processing_time_timer(
                    timer_timestamp
                )
            dirty_state.clear()
            timer_state.clear()
            self._redis_flushes += 1
            self._redis_updates_flushed += dirty_updates
            self._redis_updates_coalesced += dirty_updates - 1

        def _should_log_progress(self) -> bool:
            return self._log_every > 0 and self._processed_events % self._log_every == 0

        def _ensure_metrics_timer(self, runtime_context: Any) -> None:
            if self._metrics_flush_interval_ms <= 0 or self._metrics_timer_timestamp is not None:
                return
            timer_service = runtime_context.timer_service()
            timestamp = (
                timer_service.current_processing_time() + self._metrics_flush_interval_ms
            )
            timer_service.register_processing_time_timer(timestamp)
            self._metrics_timer_timestamp = timestamp

        def _log_metrics(self, *, force: bool = False) -> None:
            if not force and not self._should_log_progress():
                return
            if self._processed_events == self._last_log_processed_events:
                return
            now = time.perf_counter()
            elapsed_seconds = now - self._started_monotonic
            window_seconds = now - self._last_log_monotonic
            window_events = self._processed_events - self._last_log_processed_events
            metrics = {
                "subtask_index": self._subtask_index,
                "processed_events": self._processed_events,
                "valid_events": self._valid_events,
                "impressions": self._impressions,
                "clicks": self._clicks,
                "dead_letters": self._dead_letters,
                "redis_flush_mode": self._redis_flush_mode,
                "redis_flushes": self._redis_flushes,
                "redis_updates_flushed": self._redis_updates_flushed,
                "redis_updates_coalesced": self._redis_updates_coalesced,
                "elapsed_seconds": elapsed_seconds,
                "events_per_second": (
                    self._processed_events / elapsed_seconds
                    if elapsed_seconds > 0
                    else 0.0
                ),
                "window_events": window_events,
                "window_seconds": window_seconds,
                "window_events_per_second": (
                    window_events / window_seconds if window_seconds > 0 else 0.0
                ),
            }
            logger.info(f"CTR_PROCESSOR_METRICS {json.dumps(metrics, sort_keys=True)}")
            self._last_log_monotonic = now
            self._last_log_processed_events = self._processed_events

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
    logger.info(f"Added Kafka connector jar: {kafka_connector_jar}")
    if config.checkpoint_interval_ms > 0:
        env.enable_checkpointing(config.checkpoint_interval_ms)

    starting_offsets = (
        KafkaOffsetsInitializer.earliest()
        if config.starting_offsets == "earliest"
        else KafkaOffsetsInitializer.latest()
    )
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_group_id(config.consumer_group)
        .set_topics(config.impression_topic, config.click_topic)
        .set_starting_offsets(starting_offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    dead_letter_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.dead_letter_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    logger.info(
        f"Starting realtime CTR Flink topology from topics "
        f"{config.impression_topic}, {config.click_topic}"
    )
    events = env.from_source(source, WatermarkStrategy.no_watermarks(), "ctr-events")
    dead_letters = events.key_by(_raw_event_bucket_key, key_type=Types.STRING()).process(
        RedisCtrProcessFunction(
            REDIS_URL,
            config.log_every,
            config.metrics_flush_interval_ms,
            config.redis_flush_mode,
            config.redis_flush_interval_ms,
            config.redis_flush_max_updates,
        ),
        output_type=Types.STRING(),
    )
    dead_letters.sink_to(dead_letter_sink).name("dead-letter-sink")
    with _suppress_py4j_keyboard_interrupt_log():
        env.execute("ctx-ctr-realtime-ctr")


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in (
        "impression_topic",
        "click_topic",
        "dead_letter_topic",
        "consumer_group",
        "starting_offsets",
        "parallelism",
        "checkpoint_interval_ms",
        "kafka_connector_jar",
        "log_every",
        "metrics_flush_interval_ms",
        "redis_flush_mode",
        "redis_flush_interval_ms",
        "redis_flush_max_updates",
        "trust_z_score",
        "trust_min_impressions",
        "trust_max_variance",
        "trust_max_ci_width",
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return overrides


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


def _raw_event_bucket_key(raw_event: str) -> str:
    try:
        payload = json.loads(raw_event)
        return (
            f"{payload['ad_category']}:"
            f"{payload['publisher_domain']}:"
            f"{payload['conversation_category']}"
        )
    except (KeyError, TypeError, json.JSONDecodeError):
        return INVALID_BUCKET_KEY


def _dead_letter_json(reason: str, event: dict[str, Any]) -> str:
    payload = DeadLetterPayload(reason=reason, event=event)
    return payload.model_dump_json()


if __name__ == "__main__":
    main()
