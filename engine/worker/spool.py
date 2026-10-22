"""Per-call event spool: every worker event is appended to disk before it is sent, then posted to the API
in order and retried without limit until acknowledged. A worker restart replays what was not acknowledged.

Files (one pair per call, under ``SPOOL_DIR``): ``<call_id>.jsonl`` (one event per line) and
``<call_id>.ack`` (the last acknowledged seq). Both are deleted once the call has ended and everything is
acknowledged. Acknowledged means a 2xx, or a 4xx other than 408/429 (the API will never accept it).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Optional

import httpx

from engine.config import Settings

logger = logging.getLogger(__name__)

BACKOFF_BASE_S = 0.5
BACKOFF_MAX_S = 10.0


def _retryable(status: Optional[int]) -> bool:
    return status is None or status >= 500 or status in (408, 429)


class EventSpool:
    def __init__(self, settings: Settings, http: httpx.AsyncClient, call_id: str, *, directory: str,
                 registry: Optional[dict[str, "EventSpool"]] = None):
        self.settings = settings
        self.http = http
        self.call_id = call_id
        self.directory = directory
        self.path = os.path.join(directory, f"{call_id}.jsonl")
        self.ack_path = os.path.join(directory, f"{call_id}.ack")
        self._registry = registry
        self._events: list[dict[str, Any]] = []
        self._acked_seq = 0
        self._ended = False
        self._loaded = False
        self._task: Optional[asyncio.Task] = None
        self._nudge = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        if registry is not None:
            registry[call_id] = self

    # ------------------------------------------------------------------ producer side
    def append(self, event: dict[str, Any]) -> None:
        os.makedirs(self.directory, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._events.append(event)
        self._idle.clear()
        self._nudge.set()

    def mark_ended(self) -> None:
        self._ended = True
        self._idle.clear()          # idle again only once the sender has drained and cleaned up
        self._nudge.set()

    @property
    def pending(self) -> int:
        return len(self._events)

    async def wait_idle(self, timeout: Optional[float]) -> bool:
        try:
            await asyncio.wait_for(self._idle.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # ------------------------------------------------------------------ lifecycle
    def _load(self) -> None:
        """Pick up what an earlier process left behind (replay after a worker restart)."""
        if self._loaded:
            return
        self._loaded = True
        if not os.path.exists(self.path):
            return
        try:
            with open(self.ack_path, encoding="utf-8") as f:
                self._acked_seq = int(f.read().strip() or 0)
        except (OSError, ValueError):
            self._acked_seq = 0
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if int(ev.get("seq") or 0) > self._acked_seq:
                    self._events.append(ev)
        if self._events:
            self._idle.clear()

    def start(self) -> None:
        if self._task is None:
            self._load()
            self._task = asyncio.create_task(self._run(), name=f"gvox-spool-{self.call_id}")

    async def aclose(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._registry is not None:
            self._registry.pop(self.call_id, None)

    def _write_ack(self) -> None:
        try:
            with open(self.ack_path, "w", encoding="utf-8") as f:
                f.write(str(self._acked_seq))
        except OSError as e:
            logger.warning("spool %s: cannot write ack: %s", self.call_id, e)

    def _cleanup(self) -> None:
        for p in (self.path, self.ack_path):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning("spool %s: cannot remove %s: %s", self.call_id, p, e)
        if self._registry is not None:
            self._registry.pop(self.call_id, None)

    async def _run(self) -> None:
        url = f"{self.settings.internal_api_url.rstrip('/')}/internal/calls/{self.call_id}/events"
        headers = {"X-Internal-Token": self.settings.internal_token}
        while True:
            if not self._events:
                if self._ended:
                    self._cleanup()
                    self._idle.set()
                    return
                self._idle.set()
                self._nudge.clear()
                await self._nudge.wait()
                continue
            ev = self._events[0]
            attempt = 0
            while True:
                status: Optional[int] = None
                try:
                    r = await self.http.post(url, json=ev, headers=headers, timeout=5.0)
                    status = r.status_code
                except Exception as e:
                    logger.warning("spool %s: event %s post failed: %s", self.call_id, ev.get("type"), e)
                if not _retryable(status):
                    break
                attempt += 1
                delay = min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (attempt - 1)))
                if attempt in (1, 5) or attempt % 20 == 0:
                    logger.warning("spool %s: event %s (seq %s) not acknowledged (status %s), attempt %d; retrying in %.1fs",
                                   self.call_id, ev.get("type"), ev.get("seq"), status, attempt, delay)
                await asyncio.sleep(delay)
            if status is not None and 400 <= status < 500:
                logger.warning("spool %s: event %s (seq %s) rejected with %s; dropping", self.call_id,
                               ev.get("type"), ev.get("seq"), status)
            self._events.pop(0)
            self._acked_seq = max(self._acked_seq, int(ev.get("seq") or 0))
            self._write_ack()


async def replay_pending(settings: Settings, http: httpx.AsyncClient, directory: str,
                         registry: dict[str, EventSpool]) -> int:
    """Resume every spool file left in ``directory``: unacknowledged events are re-sent, fully acknowledged
    files are removed. Returns the number of spools that had something to replay."""
    if not os.path.isdir(directory):
        return 0
    replayed = 0
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".jsonl"):
            continue
        call_id = name[: -len(".jsonl")]
        spool = EventSpool(settings, http, call_id, directory=directory, registry=registry)
        spool._load()
        if not spool.pending:
            spool._cleanup()
            continue
        logger.warning("spool %s: replaying %d unacknowledged event(s) from a previous worker run",
                       call_id, spool.pending)
        spool.start()
        spool.mark_ended()
        replayed += 1
    return replayed
