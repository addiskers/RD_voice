"""Capacity accounting: engine slots (from worker heartbeats), carrier slots and a dials-per-second budget.

Two pools are tracked separately because they fail differently: the engine runs out of conversation
slots (CPU, one worker process at its cap) while the carrier runs out of concurrent PSTN legs or
rejects dial bursts. A dialer paces on ``GET /v1/capacity`` and receives a 429 with a ``reason`` when
it overshoots.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


def parse_limits(spec: str) -> dict[str, int]:
    """``"plivo=50, vobiz=10"`` -> ``{"plivo": 50, "vobiz": 10}`` (malformed parts are skipped)."""
    out: dict[str, int] = {}
    for part in (spec or "").split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        try:
            out[k.strip().lower()] = int(v.strip())
        except ValueError:
            continue
    return out


class CpsBudget:
    """Token bucket per provider: capacity == rate, refilled at ``rate`` tokens per second."""

    def __init__(self, rate_for: Callable[[str], int]):
        self._rate_for = rate_for
        self._tokens: dict[str, float] = {}
        self._last: dict[str, float] = {}

    def _refill(self, provider: str, now: float) -> float:
        rate = max(0, int(self._rate_for(provider)))
        last = self._last.get(provider)
        tokens = self._tokens.get(provider, float(rate))
        if last is not None and rate > 0:
            tokens = min(float(rate), tokens + (now - last) * rate)
        self._last[provider] = now
        self._tokens[provider] = tokens
        return tokens

    def available(self, provider: str, now: Optional[float] = None) -> int:
        return int(self._refill(provider.lower(), time.monotonic() if now is None else now))

    def try_acquire(self, provider: str, now: Optional[float] = None) -> bool:
        key = provider.lower()
        tokens = self._refill(key, time.monotonic() if now is None else now)
        if tokens >= 1.0:
            self._tokens[key] = tokens - 1.0
            return True
        return False


@dataclass
class CarrierPool:
    total: int
    free: int
    cps_free: int

    def as_dict(self) -> dict[str, int]:
        return {"total": self.total, "free": self.free, "cps_free": self.cps_free}


@dataclass
class CapacityView:
    engine_total: int
    engine_free: int
    carriers: dict[str, CarrierPool] = field(default_factory=dict)
    workers: list[dict[str, Any]] = field(default_factory=list)
    legacy_active: int = 0

    @property
    def available(self) -> int:
        pools = [self.engine_free] + [p.free for p in self.carriers.values()]
        return max(0, min(pools)) if pools else 0
