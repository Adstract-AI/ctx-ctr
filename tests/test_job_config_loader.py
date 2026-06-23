from __future__ import annotations

from pathlib import Path

import pytest

from ctx_ctr.job_config_loader import JobConfigError, load_job_config, merge_job_config
from ctx_ctr.models.job_configs import (
    CleanTopicsJobConfig,
    ProduceEventsJobConfig,
    ResetValuesJobConfig,
    RunWeightUpdateJobConfig,
    RunRealtimeCtrJobConfig,
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
                "bootstrap_servers: localhost:9092",
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


def test_default_job_configs_are_valid() -> None:
    config_specs = [
        ("configs/seed_values.yaml", SeedValuesJobConfig),
        ("configs/reset_values.yaml", ResetValuesJobConfig),
        ("configs/clean_topics.yaml", CleanTopicsJobConfig),
        ("configs/produce_events.yaml", ProduceEventsJobConfig),
        ("configs/run_realtime_ctr.yaml", RunRealtimeCtrJobConfig),
        ("configs/run_weight_update.yaml", RunWeightUpdateJobConfig),
    ]

    for config_path, model_type in config_specs:
        assert load_job_config(config_path, model_type)
