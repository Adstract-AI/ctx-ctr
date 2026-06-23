"""PostgreSQL adapter for Task 2 model-snapshot history."""

from __future__ import annotations

from types import TracebackType
from typing import Any
from typing import Self

import psycopg
from psycopg.types.json import Jsonb

from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import SeedModelSnapshot
from ctx_ctr.models.weight_update import WeightUpdateRunMetrics

logger = get_logger(__name__)


class PostgresRuntimeAdapter:
    """Insert Task 2 model snapshots into PostgreSQL."""

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

    def _require_connection(self) -> psycopg.Connection[tuple[Any, ...]]:
        if self._connection is None:
            raise WeightUpdateError("Postgres runtime adapter must be opened before use")
        return self._connection
