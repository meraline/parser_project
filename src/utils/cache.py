from __future__ import annotations

"""Simple Redis cache wrapper."""

from typing import Optional

import redis


class RedisCache:
    """Wrapper around Redis client using URL for configuration."""

    def __init__(self, url: str) -> None:
        self._client = redis.from_url(url, decode_responses=True)

    def get(self, key: str) -> Optional[str]:
        """Return value from cache or ``None`` if key is missing."""
        return self._client.get(key)

    def set(self, key: str, value: str, ex: Optional[int] = None) -> None:
        """Store value in cache with optional expiration."""
        self._client.set(key, value, ex=ex)


__all__ = ["RedisCache"]
