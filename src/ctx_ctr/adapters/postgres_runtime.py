"""PostgreSQL adapter for Task 2 model-snapshot history."""

from __future__ import annotations

from types import TracebackType
from typing import Any
from typing import Self

import psycopg
from psycopg.types.json import Jsonb

from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.experiment import JsonObject
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot
from ctx_ctr.models.weight_update import WeightUpdateRunMetrics

logger = get_logger(__name__)


class PostgresRuntimeAdapter:
    """Persist runtime model snapshots and bucket statistics into PostgreSQL."""

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

    def insert_model_snapshot(
        self,
        snapshot: SeedModelSnapshot,
        metrics: WeightUpdateRunMetrics,
    ) -> int:
        """Insert one Task 2 model snapshot row and return its database id."""

        connection = self._require_connection()
        metrics_payload = snapshot.metrics.model_dump() | {
            "weight_update": metrics.model_dump(),
        }
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO ctr_model_snapshots (
                        snapshot_name,
                        w0,
                        weights,
                        metrics
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        snapshot.snapshot_name,
                        snapshot.w0,
                        Jsonb(snapshot.weights.model_dump()),
                        Jsonb(metrics_payload),
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        except psycopg.Error as error:
            connection.rollback()
            raise WeightUpdateError("Failed to insert PostgreSQL model snapshot") from error

        if row is None:
            raise WeightUpdateError("PostgreSQL model snapshot insert did not return an id")

        snapshot_id = int(row[0])
        logger.debug(
            f"Inserted PostgreSQL model snapshot {snapshot.snapshot_name} with id {snapshot_id}"
        )
        return snapshot_id

    def upsert_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Insert or replace runtime CTR bucket statistics."""

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
            logger.debug(f"Persisted {len(buckets)} PostgreSQL bucket statistics")
        except psycopg.Error as error:
            connection.rollback()
            raise WeightUpdateError("Failed to persist PostgreSQL bucket statistics") from error

    def count_model_snapshots(self) -> int:
        """Return the number of model snapshots currently stored."""

        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM ctr_model_snapshots")
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise WeightUpdateError("Failed to count PostgreSQL model snapshots") from error
        if row is None:
            raise WeightUpdateError("PostgreSQL model snapshot count returned no rows")
        return int(row[0])

    def count_experiment_results(self) -> int:
        """Return the number of experiment result rows currently stored."""

        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM ctr_experiment_results")
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise WeightUpdateError("Failed to count PostgreSQL experiment results") from error
        if row is None:
            raise WeightUpdateError("PostgreSQL experiment result count returned no rows")
        return int(row[0])

    def insert_experiment_result(
        self,
        *,
        experiment_name: str,
        config: JsonObject,
        metrics: JsonObject,
        artifact_uri: str | None,
    ) -> int:
        """Insert one experiment result row and return its database id."""

        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO ctr_experiment_results (
                        experiment_name,
                        config,
                        metrics,
                        artifact_uri
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        experiment_name,
                        Jsonb(config),
                        Jsonb(metrics),
                        artifact_uri,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        except psycopg.Error as error:
            connection.rollback()
            raise WeightUpdateError("Failed to insert PostgreSQL experiment result") from error

        if row is None:
            raise WeightUpdateError("PostgreSQL experiment result insert did not return an id")

        experiment_id = int(row[0])
        logger.debug(f"Inserted PostgreSQL experiment result {experiment_name} with id {experiment_id}")
        return experiment_id

    def _require_connection(self) -> psycopg.Connection[tuple[Any, ...]]:
        if self._connection is None:
            raise WeightUpdateError("Postgres runtime adapter must be opened before use")
        return self._connection
