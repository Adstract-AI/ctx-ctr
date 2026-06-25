from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from types import TracebackType

import pytest

from ctx_ctr.jobs import run_weight_update
from ctx_ctr.models.seed import SeedModelMetrics, SeedModelSnapshot, SeedWeights
from ctx_ctr.models.weight_update import (
    WeightUpdateResult,
    WeightUpdateRunConfig,
    WeightUpdateRunMetrics,
)


class FakePostgresContext(AbstractContextManager[object | None]):
    def __init__(self, writer: object | None) -> None:
        self.writer = writer
        self.entered = False
        self.exited = False

    def __enter__(self) -> object | None:
        self.entered = True
        return self.writer

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exited = True


class FakeWeightUpdateService:
    created: list["FakeWeightUpdateService"] = []

    def __init__(self, redis_store: object, postgres_writer: object | None) -> None:
        self.redis_store = redis_store
        self.postgres_writer = postgres_writer
        self.run_configs: list[WeightUpdateRunConfig] = []
        FakeWeightUpdateService.created.append(self)

    def recalibrate(self, config: WeightUpdateRunConfig) -> WeightUpdateResult:
        self.run_configs.append(config)
        return build_result(config)


def test_run_weight_update_uses_yaml_config_and_cli_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    config_path = tmp_path / "run_weight_update.yaml"
    config_path.write_text(
        "\n".join(
            [
                "interval_seconds: 99",
                "once: false",
                "learning_rate: 0.11",
                "evidence_smoothing: 222.0",
                "ridge: 0.03",
                "max_delta: 0.04",
                "min_feature_impressions: 222",
                "max_feature_ci_width: 0.03",
                "snapshot_name_prefix: yaml_prefix",
                "baseline_update: true",
                "baseline_max_ci_width: 0.06",
                "dry_run: false",
            ]
        ),
        encoding="utf-8",
    )
    calls: dict[str, object] = {}
    postgres_context = FakePostgresContext(writer=object())
    monkeypatch.setattr(run_weight_update, "REDIS_URL", "redis://env:6379/0")
    monkeypatch.setattr(run_weight_update, "POSTGRES_DSN", "postgresql://env")

    monkeypatch.setattr(
        run_weight_update,
        "_build_redis_store",
        lambda redis_url: calls.setdefault("redis_url", redis_url),
    )
    monkeypatch.setattr(
        run_weight_update,
        "_open_postgres_writer",
        lambda postgres_dsn, dry_run: (
            calls.setdefault("postgres_dsn", postgres_dsn),
            calls.setdefault("dry_run", dry_run),
            postgres_context,
        )[-1],
    )
    monkeypatch.setattr(run_weight_update, "WeightUpdateService", FakeWeightUpdateService)
    FakeWeightUpdateService.created = []

    run_weight_update.main(
        [
            "--config",
            str(config_path),
            "--once",
            "--dry-run",
            "--learning-rate",
            "0.25",
            "--no-baseline-update",
            "--baseline-max-ci-width",
            "0.07",
        ]
    )

    output = capsys.readouterr().out
    service = FakeWeightUpdateService.created[-1]
    run_config = service.run_configs[-1]

    assert calls["redis_url"] == "redis://env:6379/0"
    assert calls["postgres_dsn"] == "postgresql://env"
    assert calls["dry_run"] is True
    assert postgres_context.entered is True
    assert postgres_context.exited is True
    assert run_config.learning_rate == 0.25
    assert run_config.evidence_smoothing == 222.0
    assert run_config.ridge == 0.03
    assert run_config.max_delta == 0.04
    assert run_config.min_feature_impressions == 222
    assert run_config.max_feature_ci_width == 0.03
    assert run_config.snapshot_name_prefix == "yaml_prefix"
    assert run_config.baseline_update is False
    assert run_config.baseline_max_ci_width == 0.07
    assert run_config.dry_run is True
    assert "Weight update dry-run" in output
    assert "Baseline Update Enabled" in output
    assert "False" in output
    assert "Config" in output
    assert str(config_path) in output


def test_run_weight_update_disabled_once_enters_periodic_loop_until_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "run_weight_update.yaml"
    config_path.write_text(
        "\n".join(
            [
                "interval_seconds: 1",
                "once: false",
                "learning_rate: 0.25",
                "evidence_smoothing: 1000.0",
                "ridge: 0.01",
                "max_delta: 0.25",
                "min_feature_impressions: 500",
                "max_feature_ci_width: 0.02",
                "snapshot_name_prefix: yaml_prefix",
                "baseline_update: true",
                "baseline_max_ci_width: 0.02",
                "dry_run: true",
            ]
        ),
        encoding="utf-8",
    )
    postgres_context = FakePostgresContext(writer=None)

    monkeypatch.setattr(run_weight_update, "_build_redis_store", lambda redis_url: object())
    monkeypatch.setattr(
        run_weight_update,
        "_open_postgres_writer",
        lambda postgres_dsn, dry_run: postgres_context,
    )
    monkeypatch.setattr(run_weight_update, "WeightUpdateService", FakeWeightUpdateService)
    monkeypatch.setattr(
        run_weight_update.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    FakeWeightUpdateService.created = []

    run_weight_update.main(["--config", str(config_path)])

    assert len(FakeWeightUpdateService.created) == 1
    assert len(FakeWeightUpdateService.created[0].run_configs) == 1
    assert postgres_context.exited is True


def test_run_weight_update_invalid_yaml_config_fails_before_building_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "run_weight_update.yaml"
    config_path.write_text("learning_rate: 0\n", encoding="utf-8")
    called = False

    def fake_build_redis_store(_redis_url: str) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(run_weight_update, "_build_redis_store", fake_build_redis_store)

    with pytest.raises(Exception):
        run_weight_update.main(["--config", str(config_path), "--once"])

    assert called is False


def build_result(config: WeightUpdateRunConfig) -> WeightUpdateResult:
    snapshot = SeedModelSnapshot(
        snapshot_name=f"{config.snapshot_name_prefix}_20260101_000000",
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
    return WeightUpdateResult(
        accepted=True,
        dry_run=config.dry_run,
        model_snapshot=snapshot,
        metrics=WeightUpdateRunMetrics(
            input_bucket_count=3,
            valid_bucket_count=3,
            invalid_bucket_count=0,
            ad_feature_bucket_count=1,
            domain_feature_bucket_count=1,
            context_feature_bucket_count=1,
            updated_feature_bucket_count=3,
            skipped_feature_bucket_count=0,
            insufficient_impression_feature_count=0,
            wide_ci_feature_count=0,
            unknown_feature_count=0,
            max_absolute_weight_delta=0.12,
            learning_rate=config.learning_rate,
            evidence_smoothing=config.evidence_smoothing,
            ridge=config.ridge,
            max_delta=config.max_delta,
            min_feature_impressions=config.min_feature_impressions,
            max_feature_ci_width=config.max_feature_ci_width,
            dry_run=config.dry_run,
            w0_unchanged=True,
            old_w0=snapshot.w0,
            new_w0=snapshot.w0,
            baseline_delta=0.0,
            baseline_update_enabled=config.baseline_update,
            baseline_update_applied=False,
            aggregate_impressions=1200,
            aggregate_clicks=24,
            aggregate_observed_ctr=0.02,
            posterior_baseline_ctr=0.02,
            baseline_ci_low=0.01,
            baseline_ci_high=0.03,
            baseline_ci_width=0.02,
            baseline_guard_reason=(
                "baseline_guards_passed"
                if config.baseline_update
                else "baseline_update_disabled"
            ),
            valid_baseline_bucket_count=3,
            invalid_baseline_bucket_count=0,
        ),
        redis_write_applied=not config.dry_run,
        postgres_write_applied=not config.dry_run,
        snapshot_name=snapshot.snapshot_name,
        postgres_snapshot_id=None if config.dry_run else 101,
    )
