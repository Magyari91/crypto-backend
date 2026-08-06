import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class JobStatus:
    state: str
    value: Any | None = None


class AsyncJobCache:
    """Runs expensive loaders once while callers poll for the cached result."""

    def __init__(self):
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._results: dict[str, tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    async def get_or_start(
        self,
        key: str,
        ttl_seconds: int,
        loader: Callable[[], Awaitable[Any]],
    ) -> JobStatus:
        async with self._lock:
            cached = self._results.get(key)
            if cached and cached[0] > monotonic():
                return JobStatus("ready", cached[1])
            if cached:
                self._results.pop(key, None)

            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(loader())
                self._tasks[key] = task

            if not task.done():
                return JobStatus("pending")

            self._tasks.pop(key, None)
            value = task.result()
            self._results[key] = (monotonic() + ttl_seconds, value)
            return JobStatus("ready", value)

    async def wait(
        self,
        key: str,
        ttl_seconds: int,
        timeout_seconds: float,
    ) -> JobStatus:
        async with self._lock:
            task = self._tasks.get(key)
        if task is None:
            return JobStatus("missing")

        try:
            value = await asyncio.wait_for(
                asyncio.shield(task),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            return JobStatus("pending")

        async with self._lock:
            self._tasks.pop(key, None)
            self._results[key] = (monotonic() + ttl_seconds, value)
        return JobStatus("ready", value)
