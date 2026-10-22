"""Per-provider circuit breaker (process-wide).

Used for providers that cannot sit behind LiveKit's FallbackAdapter, today Deepgram Flux (its own
end-of-turn events drive turn detection, which the adapter cannot carry). After a failure the key is
"open" for ``open_seconds``: new calls start on the substitute provider instead of trying Flux again.
"""

from __future__ import annotations

import time
from typing import Optional


class ProviderBreaker:
    def __init__(self, open_seconds: float = 60.0, threshold: int = 1):
        self.open_seconds = open_seconds
        self.threshold = max(1, threshold)
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}

    def record_failure(self, key: str, now: Optional[float] = None) -> None:
        now = time.monotonic() if now is None else now
        self._failures[key] = self._failures.get(key, 0) + 1
        if self._failures[key] >= self.threshold:
            self._opened_at[key] = now

    def record_success(self, key: str) -> None:
        self._failures.pop(key, None)
        self._opened_at.pop(key, None)

    def is_open(self, key: str, now: Optional[float] = None) -> bool:
        opened = self._opened_at.get(key)
        if opened is None:
            return False
        now = time.monotonic() if now is None else now
        if now - opened >= self.open_seconds:
            self._opened_at.pop(key, None)      # half-open: let the next call try the primary again
            self._failures.pop(key, None)
            return False
        return True

    def reset(self) -> None:
        self._failures.clear()
        self._opened_at.clear()


BREAKER = ProviderBreaker()
FLUX_KEY = "deepgram:flux"
