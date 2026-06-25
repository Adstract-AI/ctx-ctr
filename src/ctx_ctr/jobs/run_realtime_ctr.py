"""Command entrypoint for running realtime CTR updates with PyFlink."""

from __future__ import annotations

import argparse
import json
import logging
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
    event_bucket_key,
)

logger = get_logger("ctx_ctr.jobs.run_realtime_ctr")

INVALID_BUCKET_KEY = "__invalid_event__"
DEFAULT_CONFIG_PATH = "configs/run_realtime_ctr.yaml"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    """Parse CLI arguments and start the realtime CTR Flink job."""

    parser = argparse.ArgumentParser(description="Run realtime CTR updates with PyFlink.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument("--impression-topic", default=None)
    parser.add_argument("--click-topic", default=None)
    parser.add_argument("--dead-letter-topic", default=None)
    parser.add_argument("--consumer-group", default=None)
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
        f"parallelism={config.parallelism}, checkpoint_interval_ms={config.checkpoint_interval_ms}, "
        f"trust_min_impressions={config.trust_min_impressions}, "
        f"trust_max_variance={config.trust_max_variance}, "
        f"trust_max_ci_width={config.trust_max_ci_width}, "
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
        """Process keyed CTR events and write valid bucket updates to Redis."""

        def __init__(self, redis_url: str, log_every: int) -> None:
            self._redis_url = redis_url
            self._log_every = log_every
            self._trust_thresholds = CtrTrustThresholds(
                z_score=config.trust_z_score,
                min_impressions=config.trust_min_impressions,
                max_variance=config.trust_max_variance,
                max_ci_width=config.trust_max_ci_width,
            )
            self._adapter: RedisCtrStateAdapter | None = None
            self._service: RealtimeCtrUpdateService | None = None
            self._bucket_state: Any | None = None
            self._valid_events = 0
            self._dead_letters = 0

        def open(self, runtime_context: Any) -> None:
            """Initialize Redis-backed model state and keyed Flink state."""

            self._adapter = RedisCtrStateAdapter(self._redis_url)
            model = self._adapter.read_current_model()
            self._service = RealtimeCtrUpdateService(model, self._trust_thresholds)
            self._bucket_state = runtime_context.get_state(
                ValueStateDescriptor("bucket_state", Types.STRING())
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

            del runtime_context
            try:
                event = CtrEvent.model_validate_json(value)
            except ValidationError:
                self._dead_letters += 1
                yield _dead_letter_json("invalid_event_payload", {"raw": value})
                return

            service = self._require_service()
            adapter = self._require_adapter()
            state = self._require_bucket_state()
            bucket = self._load_bucket_state(event, adapter, state)
            result = service.apply_event(event, bucket)

            if result.dead_letter is not None:
                self._dead_letters += 1
                yield result.dead_letter.model_dump_json()
                return

            if result.bucket is None:
                raise CtrStateError("Realtime CTR update returned no bucket for valid event")

            state.update(result.bucket.model_dump_json())
            adapter.write_bucket_statistic(result.bucket)
            self._valid_events += 1
            if self._should_log_progress():
                logger.info(
                    f"Processed {self._valid_events} valid CTR events; "
                    f"dead_letters={self._dead_letters}"
                )

        def close(self) -> None:
            """Close Redis resources."""

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

        def _should_log_progress(self) -> bool:
            return self._log_every > 0 and self._valid_events % self._log_every == 0

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

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_group_id(config.consumer_group)
        .set_topics(config.impression_topic, config.click_topic)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
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
        RedisCtrProcessFunction(REDIS_URL, config.log_every),
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
        "parallelism",
        "checkpoint_interval_ms",
        "kafka_connector_jar",
        "log_every",
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
