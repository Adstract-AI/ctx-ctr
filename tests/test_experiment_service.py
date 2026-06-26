from __future__ import annotations

from pathlib import Path

from ctx_ctr.models.experiment import ExperimentDefinition, ExperimentTrafficConfig, JsonObject
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
    assert result.metrics["delta"]["redis_total_impressions"] == 1
    assert result.metrics["before"]["current_model_snapshot_name"] == "current"


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
    assert result.metrics["producer"]["dry_run"] is True
