"""PostgreSQL persistence adapter for seed data."""

from __future__ import annotations

from types import TracebackType
from typing import Any
from typing import Self

import psycopg
from psycopg.types.json import Jsonb

from ctx_ctr.exceptions import SeedError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot, SeedRunSummary

logger = get_logger(__name__)


class PostgresSeedAdapter:
    """Persist and reset seed-owned PostgreSQL data."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._connection: psycopg.Connection[tuple[Any, ...]] | None = None

    def __enter__(self) -> Self:
        self._connection = psycopg.connect(self._dsn)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None

    def reset_seed_tables(self) -> None:
        """Clear seed-owned tables while preserving schema and raw event history."""

        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                        ctr_bucket_statistics,
                        ctr_model_snapshots,
                        ctr_experiment_results
                    RESTART IDENTITY
                    """
                )
            connection.commit()
            logger.info("Reset Postgres seed tables")
        except psycopg.Error as error:
            connection.rollback()
            raise SeedError("Failed to reset Postgres seed tables") from error

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Insert or replace seeded CTR bucket statistics."""

        if not buckets:
            return
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO ctr_bucket_statistics (
                        ad_category,
                        publisher_domain,
                        conversation_category,
                        impressions,
                        clicks,
                        alpha_prior,
                        beta_prior,
                        alpha_posterior,
                        beta_posterior,
                        ctr,
                        variance,
                        ci_low,
                        ci_high,
                        trusted
                    )
                    VALUES (
                        %(ad_category)s,
                        %(publisher_domain)s,
                        %(conversation_category)s,
                        %(impressions)s,
                        %(clicks)s,
                        %(alpha_prior)s,
                        %(beta_prior)s,
                        %(alpha_posterior)s,
                        %(beta_posterior)s,
                        %(ctr)s,
                        %(variance)s,
                        %(ci_low)s,
                        %(ci_high)s,
                        %(trusted)s
                    )
                    ON CONFLICT (
                        ad_category,
                        publisher_domain,
                        conversation_category
                    )
                    DO UPDATE SET
                        impressions = EXCLUDED.impressions,
                        clicks = EXCLUDED.clicks,
                        alpha_prior = EXCLUDED.alpha_prior,
                        beta_prior = EXCLUDED.beta_prior,
                        alpha_posterior = EXCLUDED.alpha_posterior,
                        beta_posterior = EXCLUDED.beta_posterior,
                        ctr = EXCLUDED.ctr,
                        variance = EXCLUDED.variance,
                        ci_low = EXCLUDED.ci_low,
                        ci_high = EXCLUDED.ci_high,
                        trusted = EXCLUDED.trusted,
                        updated_at = NOW()
                    """,
                    [bucket.model_dump() for bucket in buckets],
                )
            connection.commit()
            logger.info(f"Seeded {len(buckets)} Postgres bucket statistics")
        except psycopg.Error as error:
            connection.rollback()
            raise SeedError("Failed to seed Postgres bucket statistics") from error

    def insert_model_snapshots(self, snapshots: list[SeedModelSnapshot]) -> None:
        """Insert seeded model snapshots."""

        if not snapshots:
            return
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO ctr_model_snapshots (
                        snapshot_name,
                        w0,
                        weights,
                        metrics
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (
                            snapshot.snapshot_name,
                            snapshot.w0,
                            Jsonb(snapshot.weights.model_dump()),
                            Jsonb(snapshot.metrics.model_dump()),
                        )
                        for snapshot in snapshots
                    ],
                )
            connection.commit()
            logger.info(f"Seeded {len(snapshots)} Postgres model snapshots")
        except psycopg.Error as error:
            connection.rollback()
            raise SeedError("Failed to seed Postgres model snapshots") from error

    def insert_seed_run_summaries(self, summaries: list[SeedRunSummary]) -> None:
        """Insert seeded run-summary metadata."""

        if not summaries:
            return
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO ctr_experiment_results (
                        experiment_name,
                        config,
                        metrics,
                        artifact_uri
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (
                            summary.summary_name,
                            Jsonb(summary.config),
                            Jsonb(summary.metrics),
                            summary.artifact_uri,
                        )
                        for summary in summaries
                    ],
                )
            connection.commit()
            logger.info(f"Seeded {len(summaries)} Postgres seed run summaries")
        except psycopg.Error as error:
            connection.rollback()
            raise SeedError("Failed to seed Postgres seed run summaries") from error

    def _require_connection(self) -> psycopg.Connection[tuple[Any, ...]]:
        if self._connection is None:
            raise SeedError("Postgres adapter must be opened before use")
        return self._connection
