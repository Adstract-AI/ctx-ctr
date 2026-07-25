from __future__ import annotations

from pathlib import Path

from ctx_ctr.models.experiment import (
    ExperimentDefinition,
    ExperimentProcessorConfig,
    ExperimentProcessorsConfig,
    ExperimentSetupConfig,
    ExperimentSuccessGates,
    ExperimentTrafficConfig,
    ExperimentTrafficPhase,
    JsonObject,
)
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelMetrics, SeedModelSnapshot, SeedWeights
from ctx_ctr.models.weight_update import RedisBucketScanResult
from ctx_ctr.services.experiment_service import ExperimentArtifactWriter, ExperimentService


class FakeRedisReader:
    def __init__(self) -> None:
        self.scan_count = 0

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        self.scan_count += 1
        impressions = 100 + self.scan_count
        return RedisBucketScanResult(
            scanned_key_count=1,
            valid_bucket_count=1,
            invalid_bucket_count=0,
            buckets=[
                SeedBucketStatistic(
                    ad_category="finance",
                    publisher_domain="news.example",
                    conversation_category="personal_finance",
                    impressions=impressions,
                    clicks=2,
                    alpha_prior=2.0,
                    beta_prior=98.0,
                    alpha_posterior=4.0,
                    beta_posterior=196.0,
                    ctr=0.02,
                    variance=0.0001,
                    ci_low=0.01,
                    ci_high=0.03,
                    trusted=True,
                )
            ],
        )

    def read_current_weights(self) -> SeedModelSnapshot:
        return SeedModelSnapshot(
            snapshot_name="current",
            w0=-3.8918202981106265,
            weights=SeedWeights(
                w_ad={"finance": 0.0},
                w_dom={"news.example": 0.0},
                w_ctx={"personal_finance": 0.0},
            ),
            metrics=SeedModelMetrics(
                baseline_ctr=0.02,
                global_prior_strength=500.0,
                triplet_prior_strength=100.0,
                family_prior_strengths={"ad": 200.0, "domain": 200.0, "context": 150.0},
            ),
        )


class FakePostgresStore:
    def __init__(self) -> None:
        self.inserted: list[tuple[str, JsonObject, JsonObject, str | None]] = []

    def count_model_snapshots(self) -> int:
        return 3

    def count_experiment_results(self) -> int:
        return 4 + len(self.inserted)

    def insert_experiment_result(
        self,
        *,
        experiment_name: str,
        config: JsonObject,
        metrics: JsonObject,
        artifact_uri: str | None,
    ) -> int:
        self.inserted.append((experiment_name, config, metrics, artifact_uri))
        return 99


class FakePublisher:
    def __init__(self) -> None:
        self.published = 0
        self.flushed = False

    def publish(self, event: object, *, also_unified: bool) -> None:
        self.published += 1

    def flush(self) -> None:
        self.flushed = True


class PreloadRecordingPublisher(FakePublisher):
    def __init__(self, processor_manager: "RecordingProcessorManager") -> None:
        super().__init__()
        self._processor_manager = processor_manager
        self.published_before_processor_start = True

    def publish(self, event: object, *, also_unified: bool) -> None:
        self.published_before_processor_start &= not self._processor_manager.started
        super().publish(event, also_unified=also_unified)


class RecordingSetupRunner:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def clean_topics(self) -> None:
        self.calls.append("clean_topics")

    def reset_values(self) -> None:
        self.calls.append("reset_values")

    def seed_values(self) -> None:
        self.calls.append("seed_values")


class RecordingProcessorManager:
    def __init__(self, *, fail_health_check: bool = False) -> None:
        self.fail_health_check = fail_health_check
        self.started = False
        self.stopped = False
        self.health_checks = 0

    def start_processors(self, definition: ExperimentDefinition, *, run_dir: Path) -> JsonObject:
        self.started = True
        return {
            "realtime_ctr": {
                "command": ["realtime-ctr"],
                "log_path": str(run_dir / "processors" / "realtime_ctr.log"),
                "early_exit": False,
            }
        }

    def assert_healthy(self) -> None:
        self.health_checks += 1
        if self.fail_health_check:
            from ctx_ctr.services.experiment_service import ExperimentFailure

            raise ExperimentFailure("processor_realtime_ctr_exited_early_with_code_1")

    def stop_processors(self) -> JsonObject:
        self.stopped = True
        return {
            "realtime_ctr": {
                "exit_code": 0,
                "forced": False,
                "metrics": {
                    "count": 2,
                    "records": [
                        {
                            "subtask_index": 0,
                            "processed_events": 6,
                            "events_per_second": 3.5,
                            "window_events": 6,
                            "window_events_per_second": 3.5,
                            "elapsed_seconds": 2.0,
                            "redis_flushes": 2,
                            "redis_updates_flushed": 6,
                            "redis_updates_coalesced": 4,
                        },
                        {
                            "subtask_index": 1,
                            "processed_events": 10,
                            "events_per_second": 4.5,
                            "window_events": 10,
                            "window_events_per_second": 4.5,
                            "elapsed_seconds": 2.0,
                            "redis_flushes": 3,
                            "redis_updates_flushed": 10,
                            "redis_updates_coalesced": 7,
                        },
                    ],
                    "latest": {"processed_events": 10, "events_per_second": 4.5},
                },
            }
        }


class SequenceRedisReader(FakeRedisReader):
    def __init__(self, impressions: list[int], clicks: list[int]) -> None:
        self._impressions = impressions
        self._clicks = clicks
        self.scan_count = 0

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        index = min(self.scan_count, len(self._impressions) - 1)
        self.scan_count += 1
        impressions = self._impressions[index]
        clicks = self._clicks[index]
        return RedisBucketScanResult(
            scanned_key_count=1,
            valid_bucket_count=1,
            invalid_bucket_count=0,
            buckets=[
                SeedBucketStatistic(
                    ad_category="finance",
                    publisher_domain="news.example",
                    conversation_category="personal_finance",
                    impressions=impressions,
                    clicks=clicks,
                    alpha_prior=2.0,
                    beta_prior=98.0,
                    alpha_posterior=2.0 + clicks,
                    beta_posterior=98.0 + impressions - clicks,
                    ctr=0.02,
                    variance=0.0001,
                    ci_low=0.01,
                    ci_high=0.03,
                    trusted=True,
                )
            ],
        )


class SequencePostgresStore(FakePostgresStore):
    def __init__(self, model_counts: list[int]) -> None:
        super().__init__()
        self._model_counts = model_counts
        self._model_count_calls = 0

    def count_model_snapshots(self) -> int:
        index = min(self._model_count_calls, len(self._model_counts) - 1)
        self._model_count_calls += 1
        return self._model_counts[index]


def test_experiment_service_writes_artifacts_and_inserts_postgres_result(tmp_path: Path) -> None:
    redis_reader = FakeRedisReader()
    postgres_store = FakePostgresStore()
    publisher = FakePublisher()
    service = ExperimentService(
        redis_reader=redis_reader,
        postgres_store=postgres_store,
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=publisher,
    )
    definition = ExperimentDefinition(
        experiment_name="unit_experiment",
        settle_seconds=0,
        traffic=ExperimentTrafficConfig(
            impressions=10,
            events_per_second=0,
            random_seed=42,
            log_every=0,
        ),
    )

    result = service.run(definition, dry_run=False)

    assert result.postgres_experiment_id == 99
    assert result.artifact_uri is not None
    assert Path(result.artifact_uri).exists()
    assert Path(result.artifact_uri).with_suffix(".md").exists()
    assert postgres_store.inserted[0][0] == "unit_experiment"
    assert publisher.published == result.metrics["producer"]["total_events"]
    assert publisher.flushed is True
    statistics = result.metrics["statistics"]
    assert statistics["delta"]["redis_total_impressions"] == 2
    assert statistics["before"]["current_model_snapshot_name"] == "current"
    assert result.metrics["timing"]["total_seconds"] >= 0
    assert "traffic" not in result.metrics["timing"]
    assert "setup" not in result.metrics
    assert "processors" not in result.metrics
    assert "dry_run" not in result.metrics["producer"]
    assert "dry_run" not in result.metrics["traffic"]["phases"][0]["producer"]


def test_experiment_service_dry_run_does_not_insert_postgres(tmp_path: Path) -> None:
    postgres_store = FakePostgresStore()
    service = ExperimentService(
        redis_reader=FakeRedisReader(),
        postgres_store=postgres_store,
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=FakePublisher(),
    )
    definition = ExperimentDefinition(
        experiment_name="dry_run_experiment",
        settle_seconds=0,
        traffic=ExperimentTrafficConfig(impressions=5, events_per_second=0),
    )

    result = service.run(definition, dry_run=True)

    assert result.dry_run is True
    assert result.postgres_experiment_id is None
    assert result.artifact_uri is not None
    assert Path(result.artifact_uri).exists()
    assert postgres_store.inserted == []
    assert "dry_run" not in result.metrics["producer"]


def test_full_experiment_runs_setup_processors_phases_and_strict_gates(
    tmp_path: Path,
) -> None:
    setup_runner = RecordingSetupRunner()
    processor_manager = RecordingProcessorManager()
    redis_reader = SequenceRedisReader(
        impressions=[100, 100, 105, 110, 110],
        clicks=[2, 2, 3, 4, 4],
    )
    postgres_store = SequencePostgresStore(model_counts=[1, 1, 1, 2, 2])
    service = ExperimentService(
        redis_reader=redis_reader,
        postgres_store=postgres_store,
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=FakePublisher(),
        setup_runner=setup_runner,
        processor_manager=processor_manager,
    )
    definition = _strict_definition(
        phases=[
            ExperimentTrafficPhase(
                phase_name="warmup",
                impressions=5,
                events_per_second=0,
                random_seed=1,
                settle_seconds=0,
                log_every=0,
            ),
            ExperimentTrafficPhase(
                phase_name="steady",
                impressions=5,
                events_per_second=0,
                random_seed=2,
                settle_seconds=0,
                log_every=0,
            ),
        ]
    )

    result = service.run(definition, dry_run=False)

    assert setup_runner.calls == ["clean_topics", "reset_values", "seed_values"]
    assert processor_manager.started is True
    assert processor_manager.stopped is True
    assert processor_manager.health_checks >= 1
    assert len(result.metrics["traffic"]["phases"]) == 2
    assert result.metrics["success_gates"]["passed"] is True
    assert result.metrics["success_gates"]["redis_impression_delta"] == 10
    assert result.metrics["success_gates"]["model_snapshot_delta"] == 1
    assert result.metrics["timing"]["setup"]["total_seconds"] >= 0
    assert result.metrics["timing"]["processors"]["start_seconds"] >= 0
    assert result.metrics["timing"]["teardown"]["processor_stop_seconds"] >= 0
    assert "traffic" not in result.metrics["timing"]
    realtime_metrics = result.metrics["processor_metrics"]["realtime_ctr"]
    assert len(realtime_metrics["records"]) == 2
    assert realtime_metrics["average"]["processed_events"] == 8.0
    assert realtime_metrics["average"]["events_per_second"] == 4.0
    assert realtime_metrics["aggregate"]["subtask_count"] == 2
    assert realtime_metrics["aggregate"]["processed_events"] == 16.0
    assert realtime_metrics["aggregate"]["events_per_second"] == 8.0
    assert realtime_metrics["aggregate"]["window_events_per_second"] == 8.0
    assert realtime_metrics["aggregate"]["redis_flushes"] == 5.0
    assert realtime_metrics["aggregate"]["redis_updates_flushed"] == 16.0
    assert realtime_metrics["aggregate"]["redis_updates_coalesced"] == 11.0
    assert postgres_store.inserted


def test_preload_traffic_is_published_before_processors_start(tmp_path: Path) -> None:
    processor_manager = RecordingProcessorManager()
    publisher = PreloadRecordingPublisher(processor_manager)
    service = ExperimentService(
        redis_reader=SequenceRedisReader(
            impressions=[100, 100, 101, 101],
            clicks=[2, 2, 2, 2],
        ),
        postgres_store=SequencePostgresStore(model_counts=[1, 1, 1, 1]),
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=publisher,
        setup_runner=RecordingSetupRunner(),
        processor_manager=processor_manager,
    )
    definition = _strict_definition(
        phases=[
            ExperimentTrafficPhase(
                phase_name="backlog",
                impressions=1,
                events_per_second=0,
                random_seed=42,
                settle_seconds=0,
                log_every=0,
            )
        ]
    ).model_copy(
        update={
            "traffic": _strict_definition().traffic.model_copy(
                update={"preload_before_processors": True}
            )
        }
    )

    service.run(definition, dry_run=False)

    assert publisher.published > 0
    assert publisher.published_before_processor_start is True


def test_full_experiment_records_failed_strict_gate_without_losing_artifact(
    tmp_path: Path,
) -> None:
    postgres_store = SequencePostgresStore(model_counts=[1, 1, 1])
    service = ExperimentService(
        redis_reader=SequenceRedisReader(impressions=[100, 100, 101], clicks=[2, 2, 2]),
        postgres_store=postgres_store,
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=FakePublisher(),
        setup_runner=RecordingSetupRunner(),
        processor_manager=RecordingProcessorManager(),
    )
    definition = _strict_definition(
        phases=[
            ExperimentTrafficPhase(
                phase_name="warmup",
                impressions=5,
                events_per_second=0,
                random_seed=1,
                settle_seconds=0,
                log_every=0,
            )
        ]
    )

    result = service.run(definition, dry_run=False)

    assert result.metrics["success_gates"]["passed"] is False
    assert "redis_impression_delta_mismatch" in result.metrics["success_gates"]["failures"]
    assert "model_snapshot_not_created" in result.metrics["success_gates"]["failures"]
    assert result.artifact_uri is not None
    assert Path(result.artifact_uri).exists()
    assert postgres_store.inserted


def test_full_experiment_dry_run_skips_setup_processors_and_postgres_insert(
    tmp_path: Path,
) -> None:
    setup_runner = RecordingSetupRunner()
    processor_manager = RecordingProcessorManager()
    postgres_store = SequencePostgresStore(model_counts=[1, 1])
    service = ExperimentService(
        redis_reader=SequenceRedisReader(impressions=[100, 100, 100], clicks=[2, 2, 2]),
        postgres_store=postgres_store,
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=FakePublisher(),
        setup_runner=setup_runner,
        processor_manager=processor_manager,
    )

    result = service.run(_strict_definition(), dry_run=True)

    assert setup_runner.calls == []
    assert processor_manager.started is False
    assert processor_manager.stopped is False
    assert postgres_store.inserted == []
    assert "dry_run" not in result.metrics["producer"]


def test_processor_early_exit_is_recorded_as_gate_failure(tmp_path: Path) -> None:
    processor_manager = RecordingProcessorManager(fail_health_check=True)
    service = ExperimentService(
        redis_reader=SequenceRedisReader(impressions=[100, 100], clicks=[2, 2]),
        postgres_store=SequencePostgresStore(model_counts=[1, 1]),
        artifact_writer=ExperimentArtifactWriter(str(tmp_path)),
        publisher=FakePublisher(),
        setup_runner=RecordingSetupRunner(),
        processor_manager=processor_manager,
    )

    result = service.run(_strict_definition(phases=[]), dry_run=False)

    assert processor_manager.stopped is True
    assert result.metrics["success_gates"]["passed"] is False
    assert result.metrics["success_gates"]["failures"] == [
        "processor_realtime_ctr_exited_early_with_code_1"
    ]


def _strict_definition(
    *,
    phases: list[ExperimentTrafficPhase] | None = None,
) -> ExperimentDefinition:
    return ExperimentDefinition(
        experiment_name="full_unit",
        settle_seconds=0,
        setup=ExperimentSetupConfig(
            reset_values=True,
            clean_topics=True,
            seed_values=True,
        ),
        processors=ExperimentProcessorsConfig(
            realtime_ctr=ExperimentProcessorConfig(
                enabled=True,
                command=["realtime-ctr"],
                startup_seconds=0,
            )
        ),
        traffic=ExperimentTrafficConfig(
            impressions=1,
            events_per_second=0,
            random_seed=42,
            log_every=0,
            phases=phases or [
                ExperimentTrafficPhase(
                    phase_name="default",
                    impressions=1,
                    events_per_second=0,
                    random_seed=42,
                    settle_seconds=0,
                    log_every=0,
                )
            ],
        ),
        success_gates=ExperimentSuccessGates(
            require_processor_health=True,
            require_redis_impression_delta_match=True,
            require_redis_click_delta_match=False,
            require_model_snapshot_created=True,
            max_invalid_redis_buckets=0,
        ),
    )
