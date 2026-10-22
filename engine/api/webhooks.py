"""Outgoing webhooks: HMAC-SHA256 signing, ordered per-call delivery, retry with backoff."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from engine.schemas.webhooks import WebhookEvent

logger = logging.getLogger(__name__)

RETRY_DELAYS_S = (0.5, 2.0, 8.0, 30.0, 120.0)


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), f"{timestamp}.".encode("utf-8") + body, hashlib.sha256)
    return f"v1={mac.hexdigest()}"


def verify(secret: str, timestamp: str, body: bytes, signature: str, tolerance_s: int = 300) -> bool:
    """For consumers/tests: constant-time check + replay window."""
    try:
        if abs(time.time() - float(timestamp)) > tolerance_s:
            return False
    except ValueError:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body), signature or "")


@dataclass
class Delivery:
    url: str
    secret: str
    event: WebhookEvent


class WebhookDispatcher:
    """One asyncio task per call keeps that call's events in order; calls are independent."""

    def __init__(self, timeout_s: float = 10.0, on_result=None):
        self._queues: dict[str, asyncio.Queue] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._seq: dict[str, int] = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout_s
        self._on_result = on_result          # optional async callback(delivery, ok, status, attempts)
        self._closed = False

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def close(self) -> None:
        self._closed = True
        for q in self._queues.values():
            await q.put(None)
        for t in list(self._tasks.values()):
            try:
                await asyncio.wait_for(t, timeout=5)
            except Exception:
                t.cancel()
        if self._client:
            await self._client.aclose()

    def next_seq(self, call_id: str) -> int:
        self._seq[call_id] = self._seq.get(call_id, 0) + 1
        return self._seq[call_id]

    def build_event(self, call_id: str, event_type: str, data: dict[str, Any], *,
                    agent_id: Optional[str] = None, agent_version: Optional[int] = None,
                    metadata: Optional[dict[str, Any]] = None, seq: Optional[int] = None) -> WebhookEvent:
        return WebhookEvent(
            id=f"evt_{uuid.uuid4().hex[:16]}",
            type=event_type,
            call_id=call_id,
            seq=seq if seq is not None else self.next_seq(call_id),
            timestamp=datetime.now(timezone.utc).isoformat(),
            agent_id=agent_id,
            agent_version=agent_version,
            metadata=metadata or {},
            data=data,
        )

    async def emit(self, url: Optional[str], secret: str, event: WebhookEvent) -> None:
        if not url or self._closed:
            return
        q = self._queues.get(event.call_id)
        if q is None:
            q = asyncio.Queue()
            self._queues[event.call_id] = q
            self._tasks[event.call_id] = asyncio.create_task(self._run(event.call_id, q))
        await q.put(Delivery(url=url, secret=secret, event=event))

    async def _run(self, call_id: str, q: asyncio.Queue) -> None:
        try:
            while True:
                item = await q.get()
                if item is None:
                    break
                await self._deliver(item)
                if q.empty() and item.event.type in ("call.ended", "call.failed"):
                    break
        finally:
            self._queues.pop(call_id, None)
            self._tasks.pop(call_id, None)

    async def _deliver(self, d: Delivery) -> None:
        body = json.dumps(d.event.model_dump(mode="json"), separators=(",", ":")).encode("utf-8")
        attempts = 0
        last_status: Optional[int] = None
        for delay in (0.0,) + RETRY_DELAYS_S:
            if delay:
                await asyncio.sleep(delay)
            attempts += 1
            ts = str(int(time.time()))
            headers = {
                "Content-Type": "application/json",
                "X-GVox-Signature": sign(d.secret, ts, body),
                "X-GVox-Timestamp": ts,
                "X-GVox-Event": d.event.type,
                "X-GVox-Delivery": d.event.id,
                "User-Agent": "gvox-voice-engine/0.1",
            }
            try:
                assert self._client is not None
                resp = await self._client.post(d.url, content=body, headers=headers)
                last_status = resp.status_code
                if 200 <= resp.status_code < 300:
                    if self._on_result:
                        await self._on_result(d, True, last_status, attempts)
                    return
                if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
                    logger.warning("webhook %s -> %s rejected (%s); not retrying", d.event.type, d.url,
                                   resp.status_code)
                    break
            except Exception as e:  # network errors: retry
                logger.warning("webhook %s -> %s attempt %d failed: %s", d.event.type, d.url, attempts, e)
        logger.error("webhook %s -> %s gave up after %d attempts (last status %s)", d.event.type, d.url,
                     attempts, last_status)
        if self._on_result:
            await self._on_result(d, False, last_status, attempts)
