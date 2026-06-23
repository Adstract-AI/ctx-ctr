"""Redis adapter for realtime CTR state."""

from __future__ import annotations

import json
from typing import cast

from pydantic import ValidationError
from redis import Redis
from redis.exceptions import RedisError

from ctx_ctr.constants import REDIS_CURRENT_WEIGHTS_KEY
from ctx_ctr.exceptions import CtrStateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.ctr_state import CtrBucketKey, CtrBucketStatistic, CtrModelSnapshot

logger = get_logger(__name__)


class RedisCtrStateAdapter:
    """Read and write realtime CTR state in Redis."""

    def __init__(self, redis_url: str) -> None:
        self._client: Redis = Redis.from_url(redis_url, decode_responses=True)

    def read_current_model(self) -> CtrModelSnapshot:
        """Read the current CTR model from Redis."""

        try:
            raw_payload = cast(str | None, self._client.get(REDIS_CURRENT_WEIGHTS_KEY))
        except RedisError as error:
            raise CtrStateError("Failed to read current CTR model from Redis") from error

        if raw_payload is None:
            raise CtrStateError(f"Missing Redis key {REDIS_CURRENT_WEIGHTS_KEY}")

        try:
            return CtrModelSnapshot.model_validate_json(raw_payload)
        except ValidationError as error:
            raise CtrStateError("Invalid current CTR model payload in Redis") from error

    def read_bucket_statistic(self, bucket_key: CtrBucketKey) -> CtrBucketStatistic | None:
        """Read one bucket statistic from Redis when it exists."""

        try:
            raw_payload = cast(str | None, self._client.get(bucket_key.redis_key))
        except RedisError as error:
            raise CtrStateError(f"Failed to read Redis bucket {bucket_key.redis_key}") from error

        if raw_payload is None:
            return None

        try:
            return CtrBucketStatistic.model_validate_json(raw_payload)
        except ValidationError as error:
            raise CtrStateError(f"Invalid Redis bucket payload for {bucket_key.redis_key}") from error

    def write_bucket_statistic(self, bucket: CtrBucketStatistic) -> None:
        """Write one updated bucket statistic to Redis."""

        try:
            self._client.set(bucket.redis_key, json.dumps(bucket.redis_payload(), sort_keys=True))
        except RedisError as error:
            raise CtrStateError(f"Failed to write Redis bucket {bucket.redis_key}") from error

    def close(self) -> None:
        """Close the Redis connection."""

        self._client.close()  # type: ignore[no-untyped-call]
        logger.debug("Closed realtime CTR Redis adapter")
