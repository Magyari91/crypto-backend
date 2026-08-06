import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable


@dataclass
class CacheEntry:
    value: Any
    created_at: float
    expires_at: float


class AsyncTTLCache:
    def __init__(self, stale_seconds: int = 900):
        self._entries: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._stale_seconds = stale_seconds

    async def get_or_set(
        self,
        key: str,
        ttl_seconds: int,
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:
        now = monotonic()
        entry = self._entries.get(key)
        if entry and entry.expires_at > now:
            return entry.value

        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = monotonic()
            entry = self._entries.get(key)
            if entry and entry.expires_at > now:
                return entry.value

            try:
                value = await loader()
            except Exception:
                if entry and now - entry.created_at <= self._stale_seconds:
                    return entry.value
                raise

            self._entries[key] = CacheEntry(
                value=value,
                created_at=now,
                expires_at=now + ttl_seconds,
            )
            return value
