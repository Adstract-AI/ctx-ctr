from __future__ import annotations

from pathlib import Path

import pytest

from ctx_ctr.job_config_loader import JobConfigError, load_job_config, merge_job_config
from ctx_ctr.models.experiment import ExperimentDefinition, ExperimentTrafficConfig
from ctx_ctr.models.job_configs import (
    CleanTopicsJobConfig,
    ProduceEventsJobConfig,
    PersistRedisBucketsJobConfig,
    ResetValuesJobConfig,
    RunExperimentJobConfig,
    RunWeightUpdateJobConfig,
    RunRealtimeCtrJobConfig,
    RunStreamingWeightUpdateJobConfig,
    SeedValuesJobConfig,
)


def test_load_job_config_validates_yaml_into_pydantic_model(tmp_path: Path) -> None:
    config_path = tmp_path / "produce_events.yaml"
    config_path.write_text(
        "\n".join(
            [
                "impressions: 25",
                "events_per_second: 0.5",
                "random_seed: 7",
                "log_every: 5",
                "also_unified: true",
                "dry_run: true",
                "impression_topic: ctr.impressions",
                "click_topic: ctr.clicks",
                "event_topic: ctr.events",
            ]
        ),
        encoding="utf-8",
    )

    config = load_job_config(str(config_path), ProduceEventsJobConfig)

    assert config.impressions == 25
    assert config.events_per_second == 0.5
    assert config.also_unified is True
    assert config.dry_run is True


def test_load_job_config_rejects_non_object_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "bad.yaml"
    config_path.write_text("- nope\n", encoding="utf-8")

    with pytest.raises(JobConfigError):
        load_job_config(str(config_path), ProduceEventsJobConfig)


def test_cli_overrides_are_revalidated() -> None:
    config = ProduceEventsJobConfig(impressions=10)

    merged = merge_job_config(config, {"impressions": 20, "dry_run": True})

    assert merged.impressions == 20
    assert merged.dry_run is True

    with pytest.raises(ValueError):
        merge_job_config(config, {"impressions": 0})


def test_null_is_allowed_only_for_optional_config_fields() -> None:
    clean_topics = CleanTopicsJobConfig(only=None)

    assert clean_topics.topic_names == [
        "ctr.impressions",
        "ctr.clicks",
        "ctr.events",
        "ctr.dead-letter",
    ]

    with pytest.raises(ValueError):
        ProduceEventsJobConfig(impressions=None)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        RunRealtimeCtrJobConfig(kafka_connector_jar=None)  # type: ignore[arg-type]


def test_realtime_ctr_defaults_to_project_jars_folder() -> None:
    config = RunRealtimeCtrJobConfig()

    assert config.kafka_connector_jar == "jars/flink-sql-connector-kafka-3.2.0-1.19.jar"
    assert config.starting_offsets == "latest"

    with pytest.raises(ValueError):
        RunRealtimeCtrJobConfig(starting_offsets="committed")


def test_experiment_name_rejects_paths() -> None:
    with pytest.raises(ValueError):
        RunExperimentJobConfig(experiment_name="../full_system_local")


def test_full_system_local_experiment_config_is_valid() -> None:
    definition = load_job_config(
        "experiments/configs/full_system_local.yaml",
        ExperimentDefinition,
    )

    assert definition.experiment_name == "full_system_local"
    assert definition.setup.reset_values is True
    assert definition.setup.clean_topics is True
    assert definition.setup.seed_values is True
    assert definition.processors.realtime_ctr.enabled is True
    assert definition.processors.streaming_weight_update.enabled is True
    assert len(definition.traffic.phases) == 3
    assert definition.traffic.impressions is None
    assert definition.traffic.events_per_second is None
    assert definition.success_gates.require_processor_health is True


def test_realtime_ctr_performance_baseline_config_preloads_kafka() -> None:
    definition = load_job_config(
        "experiments/configs/realtime_ctr_performance_baseline.yaml",
        ExperimentDefinition,
    )

    assert definition.experiment_name == "realtime_ctr_performance_baseline"
    assert definition.traffic.preload_before_processors is True
    assert definition.traffic.phases[0].impressions == 50000
    assert definition.traffic.phases[0].events_per_second == 0
    assert "earliest" in definition.processors.realtime_ctr.command
    assert definition.processors.streaming_weight_update.enabled is False


def test_phased_traffic_config_does_not_require_single_phase_fields() -> None:
    config = ExperimentTrafficConfig.model_validate(
        {
            "enabled": True,
            "phases": [
                {
                    "phase_name": "one",
                    "impressions": 10,
                    "events_per_second": 1,
                    "random_seed": 1,
                }
            ],
        }
    )

    assert config.impressions is None
    assert len(config.phases) == 1


def test_single_phase_traffic_config_requires_fallback_fields() -> None:
    with pytest.raises(ValueError):
        ExperimentTrafficConfig(enabled=True, random_seed=1, log_every=0, also_unified=False)


def test_default_job_configs_are_valid() -> None:
    config_specs = [
        ("configs/seed_values.yaml", SeedValuesJobConfig),
        ("configs/reset_values.yaml", ResetValuesJobConfig),
        ("configs/clean_topics.yaml", CleanTopicsJobConfig),
        ("configs/persist_redis_buckets.yaml", PersistRedisBucketsJobConfig),
        ("configs/produce_events.yaml", ProduceEventsJobConfig),
        ("configs/run_experiment.yaml", RunExperimentJobConfig),
        ("configs/run_realtime_ctr.yaml", RunRealtimeCtrJobConfig),
        ("configs/run_streaming_weight_update.yaml", RunStreamingWeightUpdateJobConfig),
        ("configs/run_weight_update.yaml", RunWeightUpdateJobConfig),
    ]

    for config_path, model_type in config_specs:
        assert load_job_config(config_path, model_type)
