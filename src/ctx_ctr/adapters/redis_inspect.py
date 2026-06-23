"""Read-only Redis adapter for inspecting project runtime values."""

from __future__ import annotations

from typing import cast

from redis import Redis
from redis.exceptions import RedisError

from ctx_ctr.exceptions import CtrStateError
from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.redis_inspect import RedisKeyValue

logger = get_logger(__name__)


class RedisInspectAdapter:
    """Read Redis keys and values for local debugging jobs."""

    def __init__(self, redis_url: str) -> None:
        self._client: Redis = Redis.from_url(redis_url, decode_responses=True)

    def scan_values(self, pattern: str, limit: int) -> list[RedisKeyValue]:
        """Return up to ``limit`` Redis key/value pairs matching ``pattern``."""

        values: list[RedisKeyValue] = []
        try:
            for key in self._client.scan_iter(pattern):
                value = cast(str | None, self._client.get(key))
                values.append(RedisKeyValue(key=str(key), value=value))
                if len(values) >= limit:
                    break
        except RedisError as error:
            raise CtrStateError(f"Failed to scan Redis keys matching {pattern}") from error

        return sorted(values, key=lambda item: item.key)

    def read_value(self, key: str) -> RedisKeyValue:
        """Read one Redis key."""

        try:
            value = cast(str | None, self._client.get(key))
        except RedisError as error:
            raise CtrStateError(f"Failed to read Redis key {key}") from error

        return RedisKeyValue(key=key, value=value)

    def close(self) -> None:
        """Close Redis resources."""

        self._client.close()  # type: ignore[no-untyped-call]
        logger.debug("Closed Redis inspect adapter")
