"""Redis adapter for Task 2 weight-update runtime state."""

from __future__ import annotations

import json

from pydantic import ValidationError
from redis import Redis
from redis.exceptions import RedisError

from ctx_ctr.constants import REDIS_CTR_KEY_PREFIX, REDIS_CURRENT_WEIGHTS_KEY
from ctx_ctr.exceptions import WeightUpdateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot
from ctx_ctr.models.weight_update import RedisBucketScanResult

logger = get_logger(__name__)


class RedisRuntimeAdapter:
    """Read and write Task 2 Redis state using the seeded JSON contracts."""

    def __init__(self, redis_url: str) -> None:
        self._client: Redis = Redis.from_url(redis_url, decode_responses=True)

    def scan_bucket_statistics(self) -> RedisBucketScanResult:
        """Scan all Redis CTR bucket keys and parse valid bucket payloads."""

        buckets: list[SeedBucketStatistic] = []
        scanned_key_count = 0
        invalid_bucket_count = 0
        try:
            for key in self._client.scan_iter(f"{REDIS_CTR_KEY_PREFIX}:*"):
                scanned_key_count += 1
                payload = self._client.get(key)
                if payload is None:
                    invalid_bucket_count += 1
                    logger.debug(f"Redis bucket key {key} disappeared before it could be read")
                    continue
                try:
                    bucket = SeedBucketStatistic.model_validate(json.loads(payload))
                except (json.JSONDecodeError, ValidationError) as error:
                    invalid_bucket_count += 1
                    logger.warning(f"Skipping invalid Redis bucket payload at {key}: {error}")
                    continue
                buckets.append(bucket)
        except RedisError as error:
            raise WeightUpdateError("Failed to read Redis CTR bucket statistics") from error

        result = RedisBucketScanResult(
            scanned_key_count=scanned_key_count,
            valid_bucket_count=len(buckets),
            invalid_bucket_count=invalid_bucket_count,
            buckets=buckets,
        )
        logger.debug(
            f"Scanned {result.scanned_key_count} Redis CTR keys with "
            f"{result.valid_bucket_count} valid buckets"
        )
        return result

    def read_current_weights(self) -> SeedModelSnapshot:
        """Read and parse the current model snapshot from Redis."""

        try:
            payload = self._client.get(REDIS_CURRENT_WEIGHTS_KEY)
        except RedisError as error:
            raise WeightUpdateError("Failed to read Redis current weights") from error

        if payload is None:
            raise WeightUpdateError("Redis current weights key was not found")

        try:
            snapshot = SeedModelSnapshot.model_validate(json.loads(payload))
        except (json.JSONDecodeError, ValidationError) as error:
            raise WeightUpdateError("Redis current weights payload is invalid") from error

        logger.debug(f"Loaded Redis current weights snapshot {snapshot.snapshot_name}")
        return snapshot

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        """Overwrite the Redis current model snapshot."""

        try:
            self._client.set(
                REDIS_CURRENT_WEIGHTS_KEY,
                json.dumps(snapshot.redis_payload(), sort_keys=True),
            )
        except RedisError as error:
            raise WeightUpdateError("Failed to write Redis current weights") from error

        logger.debug(f"Wrote Redis current weights snapshot {snapshot.snapshot_name}")
