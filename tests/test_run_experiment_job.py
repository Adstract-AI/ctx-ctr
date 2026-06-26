from __future__ import annotations

from pathlib import Path
from types import TracebackType

import pytest

from ctx_ctr.jobs import run_experiment
from ctx_ctr.models.experiment import ExperimentRunResult


class FakePostgresContext:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self) -> "FakePostgresContext":
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exited = True


class FakeExperimentService:
    created: list["FakeExperimentService"] = []

    def __init__(
        self,
        *,
        redis_reader: object,
        postgres_store: object,
        artifact_writer: object,
        publisher: object,
        setup_runner: object | None = None,
        processor_manager: object | None = None,
    ) -> None:
        self.redis_reader = redis_reader
        self.postgres_store = postgres_store
        self.artifact_writer = artifact_writer
        self.publisher = publisher
        self.setup_runner = setup_runner
        self.processor_manager = processor_manager
        self.calls: list[tuple[object, bool]] = []
        FakeExperimentService.created.append(self)

    def run(self, definition: object, *, dry_run: bool) -> ExperimentRunResult:
        self.calls.append((definition, dry_run))
        return ExperimentRunResult.model_validate(
            {
                "experiment_name": getattr(definition, "experiment_name"),
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T00:00:01Z",
                "duration_seconds": 1.0,
                "dry_run": dry_run,
                "config": {},
                "metrics": {
                    "producer": {
                        "dry_run": dry_run,
                        "impressions": 10,
                        "clicks": 1,
                        "total_events": 11,
                    },
                    "before": {"redis_valid_bucket_count": 1},
                    "after": {"redis_valid_bucket_count": 2},
                    "delta": {
                        "redis_total_impressions": 10,
                        "redis_total_clicks": 1,
                        "postgres_model_snapshot_count": 0,
                    },
                },
                "artifact_uri": "experiments/results/unit.json",
                "postgres_experiment_id": None,
            }
        )


def test_run_experiment_uses_yaml_config_and_cli_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    job_config_path = tmp_path / "run_experiment.yaml"
    experiment_config_path = tmp_path / "experiment.yaml"
    output_dir = tmp_path / "results"
    job_config_path.write_text(
        "\n".join(
            [
                "experiment_name: ignored",
                "output_dir: ignored-results",
                "dry_run: false",
            ]
        ),
        encoding="utf-8",
    )
    experiment_config_path.write_text(
        "\n".join(
            [
                "experiment_name: cli_experiment",
                "description: from test",
                "settle_seconds: 0",
                "traffic:",
                "  enabled: true",
                "  impressions: 10",
                "  events_per_second: 0",
                "  random_seed: 42",
                "  log_every: 0",
                "  also_unified: false",
            ]
        ),
        encoding="utf-8",
    )
    postgres_context = FakePostgresContext()

    monkeypatch.setattr(run_experiment, "RedisRuntimeAdapter", lambda redis_url: ("redis", redis_url))
    monkeypatch.setattr(run_experiment, "PostgresRuntimeAdapter", lambda dsn: postgres_context)
    monkeypatch.setattr(
        run_experiment,
        "_experiment_config_path",
        lambda experiment_name: experiment_config_path,
    )
    monkeypatch.setattr(run_experiment, "ExperimentService", FakeExperimentService)
    FakeExperimentService.created = []

    run_experiment.main(
        [
            "--config",
            str(job_config_path),
            "--experiment",
            "cli_experiment",
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ]
    )

    output = capsys.readouterr().out
    service = FakeExperimentService.created[-1]
    definition, dry_run = service.calls[-1]

    assert getattr(definition, "experiment_name") == "cli_experiment"
    assert dry_run is True
    assert postgres_context.entered is True
    assert postgres_context.exited is True
    assert "Experiment dry-run" in output
    assert "Produced Impressions" in output
    assert "cli_experiment" in output


def test_full_system_local_experiment_resolves_to_standard_config_path() -> None:
    path = run_experiment._experiment_config_path("full_system_local")

    assert str(path) == "experiments/configs/full_system_local.yaml"


def test_parse_processor_metrics_reads_latest_realtime_ctr_record(tmp_path: Path) -> None:
    log_path = tmp_path / "realtime_ctr.log"
    log_path.write_text(
        "\n".join(
            [
                "INFO starting",
                (
                    "\x1b[36mINFO ctx_ctr.jobs.run_realtime_ctr: CTR_PROCESSOR_METRICS "
                    '{"processed_events": 10, "events_per_second": 5.0}\x1b[0m'
                ),
                (
                    "\x1b[36mINFO ctx_ctr.jobs.run_realtime_ctr: CTR_PROCESSOR_METRICS "
                    '{"processed_events": 20, "events_per_second": 8.0}\x1b[0m'
                ),
            ]
        ),
        encoding="utf-8",
    )

    metrics = run_experiment._parse_processor_metrics(log_path)

    assert metrics["count"] == 2
    assert metrics["latest"] == {"processed_events": 20, "events_per_second": 8.0}
