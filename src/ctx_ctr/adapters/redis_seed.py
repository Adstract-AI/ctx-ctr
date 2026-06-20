"""Redis feature-store adapter for seed data."""

from __future__ import annotations

import json

from redis import Redis
from redis.exceptions import RedisError

from ctx_ctr.constants import REDIS_CTR_KEY_PREFIX, REDIS_CURRENT_WEIGHTS_KEY, REDIS_WEIGHTS_KEY_PREFIX
from ctx_ctr.exceptions import SeedError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.seed import SeedBucketStatistic, SeedModelSnapshot

logger = get_logger(__name__)


class RedisSeedAdapter:
    """Write and reset seed-owned Redis feature-store keys."""

    def __init__(self, redis_url: str) -> None:
        self._client: Redis = Redis.from_url(redis_url, decode_responses=True)

    def reset_seed_keys(self) -> None:
        """Delete CTR and weight keys owned by the project seeder."""

        try:
            keys = [
                *self._client.scan_iter(f"{REDIS_CTR_KEY_PREFIX}:*"),
                *self._client.scan_iter(f"{REDIS_WEIGHTS_KEY_PREFIX}:*"),
            ]
            if keys:
                self._client.delete(*keys)
            logger.debug(f"Reset {len(keys)} Redis seed keys")
        except RedisError as error:
            raise SeedError("Failed to reset Redis seed keys") from error

    def write_bucket_statistics(self, buckets: list[SeedBucketStatistic]) -> None:
        """Write seeded bucket statistics to Redis."""

        if not buckets:
            return
        try:
            pipeline = self._client.pipeline(transaction=False)
            for bucket in buckets:
                pipeline.set(bucket.redis_key, json.dumps(bucket.redis_payload(), sort_keys=True))
            pipeline.execute()  # type: ignore[no-untyped-call]
            logger.debug(f"Seeded {len(buckets)} Redis bucket statistics")
        except RedisError as error:
            raise SeedError("Failed to seed Redis bucket statistics") from error

    def write_current_weights(self, snapshot: SeedModelSnapshot) -> None:
        """Write the current model weights to Redis."""

        try:
            self._client.set(
                REDIS_CURRENT_WEIGHTS_KEY,
                json.dumps(snapshot.redis_payload(), sort_keys=True),
            )
            logger.debug(f"Seeded Redis current weights from {snapshot.snapshot_name}")
        except RedisError as error:
            raise SeedError("Failed to seed Redis current weights") from error
