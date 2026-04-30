from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator


class ConcurrencyLimiter:
    """Small non-blocking limiter for long-running transfer work."""

    def __init__(self, limit: int) -> None:
        self.limit = max(0, int(limit))
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active(self) -> int:
        return self._active

    @asynccontextmanager
    async def acquire_nowait(self) -> AsyncIterator[bool]:
        if self.limit <= 0:
            yield True
            return

        acquired = False
        with self._lock:
            if self._active < self.limit:
                acquired = True
                self._active += 1

        if not acquired:
            yield False
            return

        try:
            yield True
        finally:
            with self._lock:
                self._active = max(0, self._active - 1)
