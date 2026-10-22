"""Worker lifecycle: identity, heartbeat to the API, admission control, and drain-to-idle.

Drain is driven by ``POST /internal/drain`` (works on every platform). The worker stops taking new
streams, lets live calls finish, waits for its event spools to be acknowledged, and then terminates
itself: on POSIX by sending itself SIGTERM so agent-transport runs its normal shutdown with zero
sessions, on Windows by exiting directly. ``deploy/worker-entrypoint.sh`` maps a container SIGTERM to
this sequence so a ``docker stop`` never drops a live call.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import httpx

from engine.config import Settings

logger = logging.getLogger("gvox.worker.lifecycle")


def worker_dirs(settings: Settings, worker_id: str) -> tuple[str, str]:
    """``(spool_dir, recording_staging_dir)`` private to this worker. Workers share the data volume in
    compose; without per-worker subdirectories a starting worker would delete its sibling's live raw
    recordings and replay its sibling's spool."""
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in worker_id) or "worker"
    spool = os.path.join(settings.spool_dir, safe)
    staging = os.path.join(settings.recordings_dir, "staging", safe)
    os.makedirs(spool, exist_ok=True)
    os.makedirs(staging, exist_ok=True)
    return spool, staging


def default_terminate() -> None:
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    if sys.platform == "win32":
        os._exit(0)
    os.kill(os.getpid(), signal.SIGTERM)


class WorkerLifecycle:
    def __init__(self, settings: Settings, http: Optional[httpx.AsyncClient], *, worker_id: str, version: str,
                 started_at: float, terminate: Optional[Callable[[], None]] = None):
        self.settings = settings
        self.http = http
        self.worker_id = worker_id
        self.version = version
        self.started_at = started_at
        self.draining = False
        self.terminate = terminate or default_terminate
        self.poll_interval_s = 1.0
        self.grace_after_timeout_s = 10.0
        self._drain_task: Optional[asyncio.Task] = None
        self._drain_started_at: Optional[float] = None

    # ------------------------------------------------------------------ admission
    def should_refuse(self, active: int) -> Optional[str]:
        if active >= self.settings.worker_max_sessions:
            return "capacity"
        if self.draining:
            # the proxy only notices the 503 on its next health check; refusing before that fails calls the
            # carrier already routed here, so keep answering for a short grace window
            started = self._drain_started_at or 0.0
            if time.monotonic() - started >= self.settings.drain_accept_grace_seconds:
                return "draining"
        return None

    # ------------------------------------------------------------------ heartbeat
    def heartbeat_body(self, active: int, spool_pending: int) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "worker_url": self.settings.worker_internal_url,
            "version": self.version,
            "active_sessions": int(active),
            "max_sessions": int(self.settings.worker_max_sessions),
            "draining": bool(self.draining),
            "started_at": datetime.fromtimestamp(self.started_at, tz=timezone.utc).isoformat(),
            "spool_pending": int(spool_pending),
        }

    async def heartbeat_once(self, active: int, spool_pending: int) -> bool:
        if self.http is None:
            return False
        url = f"{self.settings.internal_api_url.rstrip('/')}/internal/workers/heartbeat"
        try:
            r = await self.http.post(url, json=self.heartbeat_body(active, spool_pending),
                                     headers={"X-Internal-Token": self.settings.internal_token}, timeout=5.0)
            return r.status_code == 200
        except Exception as e:
            logger.warning("heartbeat failed: %s", e)
            return False

    async def heartbeat_loop(self, active_fn: Callable[[], int], spool_pending_fn: Callable[[], int]) -> None:
        while True:
            await self.heartbeat_once(active_fn(), spool_pending_fn())
            await asyncio.sleep(self.settings.worker_heartbeat_seconds)

    # ------------------------------------------------------------------ drain
    async def begin_drain(self, handles_fn: Callable[[], list], end_fn: Optional[Callable[[Any, str], Awaitable[None]]],
                          spool_pending_fn: Callable[[], int]) -> None:
        self.draining = True
        self._drain_started_at = self._drain_started_at or time.monotonic()
        deadline = time.monotonic() + self.settings.drain_timeout_seconds
        logger.warning("worker %s draining: %d live call(s), %d spooled event(s); timeout %ds", self.worker_id,
                       len(handles_fn()), spool_pending_fn(), self.settings.drain_timeout_seconds)
        while time.monotonic() < deadline:
            if not handles_fn() and spool_pending_fn() == 0:
                logger.warning("worker %s idle; terminating", self.worker_id)
                self.terminate()
                return
            await asyncio.sleep(self.poll_interval_s)
        remaining = list(handles_fn())
        logger.error("worker %s drain timeout: ending %d call(s)", self.worker_id, len(remaining))
        for handle in remaining:
            if end_fn is None:
                break
            try:
                await end_fn(handle, "drain_timeout")
            except Exception as e:
                logger.warning("drain: ending a call failed: %s", e)
        grace = time.monotonic() + self.grace_after_timeout_s
        while time.monotonic() < grace and handles_fn():
            await asyncio.sleep(self.poll_interval_s)
        self.terminate()

    def start_drain(self, handles_fn, end_fn, spool_pending_fn) -> asyncio.Task:
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self.begin_drain(handles_fn, end_fn, spool_pending_fn),
                                                   name="gvox-drain")
        return self._drain_task
