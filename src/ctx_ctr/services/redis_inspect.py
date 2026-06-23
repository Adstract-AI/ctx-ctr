"""Service for collecting Redis values for local inspection."""

from __future__ import annotations

from ctx_ctr.adapters.redis_inspect import RedisInspectAdapter
from ctx_ctr.models.ctr_state import CtrBucketKey
from ctx_ctr.models.redis_inspect import RedisInspectSnapshot


class RedisInspectService:
    """Collect Redis key/value snapshots without modifying Redis."""

    def __init__(self, adapter: RedisInspectAdapter) -> None:
        self._adapter = adapter

    def collect_snapshot(
        self,
        *,
        pattern: str,
        limit: int,
        bucket_key: CtrBucketKey | None,
        only_bucket: bool,
    ) -> RedisInspectSnapshot:
        """Collect configured Redis values and an optional focused bucket."""

        values = [] if only_bucket else self._adapter.scan_values(pattern=pattern, limit=limit)
        bucket_value = None
        redis_bucket_key = None

        if bucket_key is not None:
            bucket_payload = self._adapter.read_value(bucket_key.redis_key)
            redis_bucket_key = bucket_payload.key
            bucket_value = bucket_payload.value

        return RedisInspectSnapshot(
            pattern=pattern,
            key_count=len(values),
            values=values,
            bucket_key=redis_bucket_key,
            bucket_value=bucket_value,
        )
