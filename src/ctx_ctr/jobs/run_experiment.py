"""Command entrypoint for running CTR experiments."""

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from ctx_ctr.adapters.kafka_event_producer import KafkaEventProducerAdapter
from ctx_ctr.adapters.postgres_runtime import PostgresRuntimeAdapter
from ctx_ctr.adapters.postgres_seed import PostgresSeedAdapter
from ctx_ctr.adapters.redis_runtime import RedisRuntimeAdapter
from ctx_ctr.adapters.redis_seed import RedisSeedAdapter
from ctx_ctr.adapters.kafka_admin import KafkaTopicAdminAdapter
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
    DEAD_LETTER_TOPIC,
    EVENT_TOPIC,
    IMPRESSION_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    LOG_COLOR,
    LOG_LEVEL,
    POSTGRES_DSN,
    REDIS_URL,
)
from ctx_ctr.job_config_loader import load_job_config, merge_job_config
from ctx_ctr.jobs.output import print_failure, print_success
from ctx_ctr.logging_config import configure_logging, get_logger
from ctx_ctr.models.events import CtrEvent
from ctx_ctr.models.experiment import ExperimentDefinition, ExperimentRunResult
from ctx_ctr.models.job_configs import RunExperimentJobConfig
from ctx_ctr.services.experiment_service import (
    ExperimentArtifactWriter,
    ExperimentFailure,
    ExperimentService,
)
from ctx_ctr.services.seed_service import SeedService
from ctx_ctr.services.seed_values_dataset import build_seed_values_dataset

logger = get_logger("ctx_ctr.jobs.run_experiment")
DEFAULT_CONFIG_PATH = "configs/run_experiment.yaml"
EXPERIMENT_CONFIG_DIR = Path("experiments/configs")
EXPERIMENT_CONFIG_SUFFIX = ".yaml"


class DryRunEventPublisher:
    """No-op event publisher used when the experiment should not write Kafka events."""

    def publish(self, event: CtrEvent, *, also_unified: bool) -> None:
        """Skip publishing one event."""

    def flush(self) -> None:
        """Skip producer flush."""


class RuntimeExperimentSetupRunner:
    """Run destructive setup actions against local runtime services."""

    def clean_topics(self) -> None:
        """Clean known CTR Kafka topics."""

        KafkaTopicAdminAdapter(
            KAFKA_BOOTSTRAP_SERVERS,
            [IMPRESSION_TOPIC, CLICK_TOPIC, EVENT_TOPIC, DEAD_LETTER_TOPIC],
        ).clean_topics()

    def reset_values(self) -> None:
        """Reset seed-owned Redis and PostgreSQL state."""

        with PostgresSeedAdapter(POSTGRES_DSN) as postgres:
            redis = RedisSeedAdapter(REDIS_URL)
            SeedService(postgres=postgres, redis=redis).reset(dry_run=False)

    def seed_values(self) -> None:
        """Seed deterministic Redis and PostgreSQL state."""

        dataset = build_seed_values_dataset()
        with PostgresSeedAdapter(POSTGRES_DSN) as postgres:
            redis = RedisSeedAdapter(REDIS_URL)
            SeedService(postgres=postgres, redis=redis).seed(dataset, dry_run=False)


@dataclass
class ManagedProcessor:
    """Bookkeeping for one processor child process."""

    name: str
    command: list[str]
    process: subprocess.Popen[str]
    log_path: Path
    log_handle: TextIO
    started_at: datetime
    stop_timeout_seconds: float


class SubprocessExperimentProcessorManager:
    """Start, monitor, log, and stop experiment child processors."""

    def __init__(self) -> None:
        self._processors: list[ManagedProcessor] = []

    def start_processors(self, definition: ExperimentDefinition, *, run_dir: Path) -> dict[str, object]:
        """Start enabled processors and capture their output under the run directory."""

        processor_dir = run_dir / "processors"
        processor_dir.mkdir(parents=True, exist_ok=True)
        processors = {
            "realtime_ctr": definition.processors.realtime_ctr,
            "streaming_weight_update": definition.processors.streaming_weight_update,
        }
        started: dict[str, object] = {}
        for name, config in processors.items():
            if not config.enabled:
                continue
            command = self._resolve_command(config.command)
            log_path = processor_dir / f"{name}.log"
            log_handle = log_path.open("w", encoding="utf-8")
            process = subprocess.Popen(  # noqa: S603
                command,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
            managed = ManagedProcessor(
                name=name,
                command=command,
                process=process,
                log_path=log_path,
                log_handle=log_handle,
                started_at=datetime.now(tz=UTC),
                stop_timeout_seconds=config.stop_timeout_seconds,
            )
            self._processors.append(managed)
            started[name] = {
                "command": command,
                "pid": process.pid,
                "log_path": str(log_path),
                "started_at": managed.started_at.isoformat(),
                "early_exit": False,
            }
            logger.info(f"Started experiment processor {name} with pid {process.pid}")
        return started

    def assert_healthy(self) -> None:
        """Raise if any managed processor has exited before teardown."""

        for processor in self._processors:
            exit_code = processor.process.poll()
            if exit_code is not None:
                raise ExperimentFailure(
                    f"processor_{processor.name}_exited_early_with_code_{exit_code}"
                )

    def stop_processors(self) -> dict[str, object]:
        """Gracefully stop all managed processors and return their exit metadata."""

        stopped: dict[str, object] = {}
        for processor in self._processors:
            stopped[processor.name] = self._stop_processor(processor)
        self._processors = []
        return stopped

    def _stop_processor(self, processor: ManagedProcessor) -> dict[str, object]:
        exit_code = processor.process.poll()
        forced = False
        if exit_code is None:
            processor.process.send_signal(signal.SIGINT)
            try:
                exit_code = processor.process.wait(timeout=processor.stop_timeout_seconds)
            except subprocess.TimeoutExpired:
                forced = True
                processor.process.kill()
                exit_code = processor.process.wait(timeout=5)
        processor.log_handle.close()
        stopped_at = datetime.now(tz=UTC)
        logger.info(
            f"Stopped experiment processor {processor.name}: "
            f"exit_code={exit_code}, forced={forced}"
        )
        return {
            "pid": processor.process.pid,
            "command": processor.command,
            "log_path": str(processor.log_path),
            "started_at": processor.started_at.isoformat(),
            "stopped_at": stopped_at.isoformat(),
            "exit_code": exit_code,
            "forced": forced,
        }

    def _resolve_command(self, command: list[str]) -> list[str]:
        if command and command[0] == "{python}":
            return [sys.executable, *command[1:]]
        return command


def main(argv: Sequence[str] | None = None) -> None:
    """Parse CLI arguments and run one CTR experiment."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    file_config = load_job_config(args.config, RunExperimentJobConfig)
    config = merge_job_config(file_config, _cli_overrides(args))
    experiment_config_path = _experiment_config_path(config.experiment_name)
    definition = load_job_config(str(experiment_config_path), ExperimentDefinition)
    configure_logging(LOG_LEVEL, LOG_COLOR)

    logger.info(
        f"Configured experiment job: experiment={definition.experiment_name}, "
        f"traffic_enabled={definition.traffic.enabled}, dry_run={config.dry_run}, "
        f"experiment_config={experiment_config_path}, output_dir={config.output_dir}, "
        f"config={args.config}"
    )

    producer: KafkaEventProducerAdapter | DryRunEventPublisher | None = None
    try:
        producer = _build_publisher(config=config, definition=definition)
        redis_reader = RedisRuntimeAdapter(REDIS_URL)
        artifact_writer = ExperimentArtifactWriter(config.output_dir)
        with PostgresRuntimeAdapter(POSTGRES_DSN) as postgres_store:
            service = ExperimentService(
                redis_reader=redis_reader,
                postgres_store=postgres_store,
                artifact_writer=artifact_writer,
                publisher=producer,
                setup_runner=RuntimeExperimentSetupRunner(),
                processor_manager=SubprocessExperimentProcessorManager(),
            )
            result = service.run(definition, dry_run=config.dry_run)
    except Exception as error:
        print_failure("Experiment", error)
        raise
    finally:
        if isinstance(producer, KafkaEventProducerAdapter):
            producer.close()

    success_gates = result.metrics.get("success_gates")
    if isinstance(success_gates, dict) and success_gates.get("passed") is False:
        failures = ", ".join(str(item) for item in success_gates.get("failures", []))
        gate_error = ExperimentFailure(f"Experiment success gates failed: {failures}")
        print_failure("Experiment", gate_error)
        raise gate_error
    print_success(
        "Experiment complete" if not result.dry_run else "Experiment dry-run",
        _result_lines(result),
    )


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the experiment runner."""

    parser = argparse.ArgumentParser(description="Run one CTR experiment.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="path to the job YAML config")
    parser.add_argument(
        "--experiment",
        default=None,
        help="experiment name from experiments/configs/{name}.yaml",
    )
    parser.add_argument("--output-dir", default=None, help="directory for experiment artifacts")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="simulate the experiment without writing Kafka or PostgreSQL",
    )
    return parser


def _cli_overrides(args: argparse.Namespace) -> dict[str, object]:
    """Return CLI values explicitly overriding the YAML config."""

    overrides: dict[str, object] = {}
    for field_name in ("experiment", "output_dir", "dry_run"):
        value = getattr(args, field_name)
        if value is not None:
            overrides["experiment_name" if field_name == "experiment" else field_name] = value
    return overrides


def _experiment_config_path(experiment_name: str) -> Path:
    """Resolve an experiment name to its YAML definition path."""

    return EXPERIMENT_CONFIG_DIR / f"{experiment_name}{EXPERIMENT_CONFIG_SUFFIX}"


def _build_publisher(
    *,
    config: RunExperimentJobConfig,
    definition: ExperimentDefinition,
) -> KafkaEventProducerAdapter | DryRunEventPublisher:
    """Construct the event publisher for this run."""

    if config.dry_run or not definition.traffic.enabled:
        return DryRunEventPublisher()
    return KafkaEventProducerAdapter(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        impression_topic=IMPRESSION_TOPIC,
        click_topic=CLICK_TOPIC,
        event_topic=EVENT_TOPIC,
    )


def _result_lines(result: ExperimentRunResult) -> list[str]:
    metrics = result.metrics
    producer = metrics["producer"]
    before = metrics["before"]
    after = metrics["after"]
    delta = metrics["delta"]
    success_gates = metrics.get("success_gates", {})
    timing = metrics.get("timing", {})
    traffic_timing = _metric_value(timing, "traffic")
    teardown_timing = _metric_value(timing, "teardown")
    return [
        f"experiment: {result.experiment_name}",
        f"dry run: {result.dry_run}",
        f"duration seconds: {result.duration_seconds:.3f}",
        f"timed total seconds: {_metric_value(timing, 'total_seconds')}",
        f"traffic produce seconds: {_metric_value(traffic_timing, 'produce_seconds')}",
        "observed events/sec: "
        f"{_metric_value(traffic_timing, 'observed_events_per_second')}",
        "processor stop seconds: "
        f"{_metric_value(teardown_timing, 'processor_stop_seconds')}",
        f"produced impressions: {_metric_value(producer, 'impressions')}",
        f"produced clicks: {_metric_value(producer, 'clicks')}",
        f"produced total events: {_metric_value(producer, 'total_events')}",
        f"redis buckets before/after: "
        f"{_metric_value(before, 'redis_valid_bucket_count')}/"
        f"{_metric_value(after, 'redis_valid_bucket_count')}",
        f"redis impression delta: {_metric_value(delta, 'redis_total_impressions')}",
        f"redis click delta: {_metric_value(delta, 'redis_total_clicks')}",
        f"postgres model snapshot delta: {_metric_value(delta, 'postgres_model_snapshot_count')}",
        f"postgres experiment id: {result.postgres_experiment_id or 'n/a'}",
        f"success gates passed: {_metric_value(success_gates, 'passed')}",
        f"success gate failures: {_metric_value(success_gates, 'failures')}",
        f"artifact: {result.artifact_uri or 'n/a'}",
    ]


def _metric_value(payload: object, key: str) -> object:
    if isinstance(payload, dict):
        return payload.get(key, "n/a")
    return "n/a"


if __name__ == "__main__":
    main()
