from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from types import TracebackType

from ctx_ctr.jobs import persist_redis_buckets
from ctx_ctr.models.persist_redis_buckets import PersistRedisBucketsResult


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


class FakePersistService:
    created: list["FakePersistService"] = []

    def __init__(self, redis_source: object, postgres_writer: object | None) -> None:
        self.redis_source = redis_source
        self.postgres_writer = postgres_writer
        self.dry_runs: list[bool] = []
        FakePersistService.created.append(self)

    def persist(self, *, dry_run: bool) -> PersistRedisBucketsResult:
        self.dry_runs.append(dry_run)
        return PersistRedisBucketsResult(
            dry_run=dry_run,
            scanned_key_count=3,
            valid_bucket_count=2,
            invalid_bucket_count=1,
            persisted_bucket_count=0 if dry_run else 2,
            postgres_write_applied=not dry_run,
        )


def test_persist_redis_buckets_uses_yaml_config_and_cli_overrides(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:  # type: ignore[no-untyped-def]
    config_path = tmp_path / "persist_redis_buckets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "redis_url: redis://yaml:6379/0",
                "postgres_dsn: postgresql://yaml",
                "dry_run: false",
            ]
        ),
        encoding="utf-8",
    )
    calls: dict[str, object] = {}
    postgres_context = FakePostgresContext(writer=object())

    monkeypatch.setattr(
        persist_redis_buckets,
        "_build_redis_source",
        lambda redis_url: calls.setdefault("redis_url", redis_url),
    )
    monkeypatch.setattr(
        persist_redis_buckets,
        "_open_postgres_writer",
        lambda postgres_dsn, dry_run: (
            calls.setdefault("postgres_dsn", postgres_dsn),
            calls.setdefault("dry_run", dry_run),
            postgres_context,
        )[-1],
    )
    monkeypatch.setattr(
        persist_redis_buckets,
        "PersistRedisBucketsService",
        FakePersistService,
    )
    FakePersistService.created = []

    persist_redis_buckets.main(
        [
            "--config",
            str(config_path),
            "--redis-url",
            "redis://cli:6379/0",
            "--dry-run",
        ]
    )

    output = capsys.readouterr().out
    service = FakePersistService.created[-1]

    assert calls["redis_url"] == "redis://cli:6379/0"
    assert calls["postgres_dsn"] == "postgresql://yaml"
    assert calls["dry_run"] is True
    assert postgres_context.entered is True
    assert postgres_context.exited is True
    assert service.dry_runs == [True]
    assert "Redis bucket persistence dry-run" in output
    assert "Redis Bucket Keys Scanned" in output
    assert str(config_path) in output
