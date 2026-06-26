"""Command entrypoint for running CTR experiments."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from ctx_ctr.adapters.kafka_event_producer import KafkaEventProducerAdapter
from ctx_ctr.adapters.postgres_runtime import PostgresRuntimeAdapter
from ctx_ctr.adapters.redis_runtime import RedisRuntimeAdapter
from ctx_ctr.env_variables import (
    CLICK_TOPIC,
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
from ctx_ctr.services.experiment_service import ExperimentArtifactWriter, ExperimentService

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
            )
            result = service.run(definition, dry_run=config.dry_run)
    except Exception as error:
        print_failure("Experiment", error)
        raise
    finally:
        if isinstance(producer, KafkaEventProducerAdapter):
            producer.close()

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
    return [
        f"experiment: {result.experiment_name}",
        f"dry run: {result.dry_run}",
        f"duration seconds: {result.duration_seconds:.3f}",
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
        f"artifact: {result.artifact_uri or 'n/a'}",
    ]


def _metric_value(payload: object, key: str) -> object:
    if isinstance(payload, dict):
        return payload.get(key, "n/a")
    return "n/a"


if __name__ == "__main__":
    main()
