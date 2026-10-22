"""Outgoing webhooks: HMAC-SHA256 signing and a database-backed outbox.

Every event is a row in ``webhook_deliveries`` before anything is sent, so an API restart loses nothing.
Per call, only the lowest pending ``seq`` is eligible at a time (strict ordering); calls are delivered
concurrently. Retries follow ``RETRY_DELAYS_S``; a 4xx other than 408/429, or exhausting the retries,
marks the row ``dead`` (``POST /v1/calls/{id}/deliveries/{seq}/replay`` resets it).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from engine.api.db import Call, Database, WebhookDelivery, utcnow
from engine.schemas.webhooks import WebhookEvent

logger = logging.getLogger(__name__)

RETRY_DELAYS_S = (0.5, 2.0, 8.0, 30.0, 120.0)
NON_RETRYABLE_4XX = lambda code: 400 <= code < 500 and code not in (408, 429)  # noqa: E731


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


def build_event(call_id: str, event_type: str, data: dict[str, Any], *, seq: int,
                agent_id: Optional[str] = None, agent_version: Optional[int] = None,
                metadata: Optional[dict[str, Any]] = None) -> WebhookEvent:
    return WebhookEvent(
        id=f"evt_{uuid.uuid4().hex[:16]}", type=event_type, call_id=call_id, seq=seq,
        timestamp=datetime.now(timezone.utc).isoformat(), agent_id=agent_id, agent_version=agent_version,
        metadata=metadata or {}, data=data,
    )


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class OutboxDispatcher:
    def __init__(self, db: Database, *, timeout_s: float = 10.0, poll_interval_s: float = 0.5,
                 http: Optional[httpx.AsyncClient] = None, secret_resolver=None):
        self.db = db
        self._timeout = timeout_s
        self._poll = poll_interval_s
        self._client: Optional[httpx.AsyncClient] = http
        self._owns_client = http is None
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._closed = False
        # maps a stored secret to the plaintext used for signing (identity until Task 10 adds encryption)
        self._resolve_secret = secret_resolver or (lambda s: s)

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        self._task = asyncio.create_task(self._loop(), name="gvox-outbox")

    async def close(self) -> None:
        self._closed = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._client and self._owns_client:
            await self._client.aclose()

    def wake(self) -> None:
        self._wake.set()

    async def _loop(self) -> None:
        while not self._closed:
            try:
                n = await self.deliver_due()
            except Exception as e:  # never let the loop die
                logger.warning("outbox pass failed: %s", e)
                n = 0
            if n:
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ producer
    async def enqueue(self, session: AsyncSession, call: Call, event_type: str, data: dict[str, Any], *,
                      agent_id: Optional[str] = None, agent_version: Optional[int] = None,
                      metadata: Optional[dict[str, Any]] = None) -> WebhookDelivery:
        call.webhook_seq = int(call.webhook_seq or 0) + 1
        event = build_event(call.id, event_type, data, seq=call.webhook_seq, agent_id=agent_id,
                            agent_version=agent_version, metadata=metadata)
        row = WebhookDelivery(
            id=event.id, call_id=call.id, seq=event.seq, event=event_type,
            payload_json=json.dumps(event.model_dump(mode="json"), separators=(",", ":"), ensure_ascii=False),
            url=call.webhook_url or "", status="pending", attempts=0, next_attempt_at=utcnow(),
        )
        session.add(row)
        await session.commit()
        self.wake()
        return row

    async def replay(self, session: AsyncSession, call_id: str, seq: int) -> bool:
        row = await session.scalar(select(WebhookDelivery).where(WebhookDelivery.call_id == call_id,
                                                                 WebhookDelivery.seq == seq))
        if row is None:
            return False
        row.status = "pending"
        row.next_attempt_at = utcnow()
        row.last_error = None
        await session.commit()
        self.wake()
        return True

    # ------------------------------------------------------------------ consumer
    async def deliver_due(self) -> int:
        """One pass: claim every call's head-of-line row that is due, send them concurrently."""
        now = utcnow()
        claimed: list[tuple[str, str, int, str, str, str]] = []
        async with self.db.session() as s:
            stmt = (select(WebhookDelivery).where(WebhookDelivery.status.in_(("pending", "delivering")))
                    .order_by(WebhookDelivery.call_id, WebhookDelivery.seq))
            if self.db.url.startswith("postgresql"):
                stmt = stmt.with_for_update(skip_locked=True)
            heads: dict[str, WebhookDelivery] = {}
            for row in await s.scalars(stmt):
                heads.setdefault(row.call_id, row)
            for row in heads.values():
                if row.status != "pending" or (_aware(row.next_attempt_at) or now) > now:
                    continue
                res = await s.execute(update(WebhookDelivery).where(WebhookDelivery.id == row.id,
                                                                     WebhookDelivery.status == "pending")
                                      .values(status="delivering"))
                if res.rowcount != 1:
                    continue
                call = await s.get(Call, row.call_id)
                secret = self._resolve_secret(call.webhook_secret or "") if call else ""
                claimed.append((row.id, row.url, row.seq, row.event, row.payload_json, secret))
            await s.commit()
        if not claimed:
            return 0
        await asyncio.gather(*(self._send(*c) for c in claimed))
        return len(claimed)

    async def _send(self, row_id: str, url: str, seq: int, event: str, payload_json: str, secret: str) -> None:
        body = payload_json.encode("utf-8")
        ts = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "X-GVox-Signature": sign(secret, ts, body),
            "X-GVox-Timestamp": ts,
            "X-GVox-Event": event,
            "X-GVox-Delivery": row_id,
            "User-Agent": "gvox-voice-engine/0.1",
        }
        status: Optional[int] = None
        error: Optional[str] = None
        try:
            assert self._client is not None
            resp = await self._client.post(url, content=body, headers=headers)
            status = resp.status_code
        except Exception as e:
            error = f"{type(e).__name__}: {e}"[:500]
        async with self.db.session() as s:
            row = await s.get(WebhookDelivery, row_id)
            if row is None:
                return
            row.attempts = int(row.attempts or 0) + 1
            row.last_status_code = status
            row.last_error = error
            if status is not None and 200 <= status < 300:
                row.status = "delivered"
                row.delivered_at = utcnow()
            elif status is not None and NON_RETRYABLE_4XX(status):
                row.status = "dead"
                logger.warning("webhook %s seq %d -> %s rejected (%s); not retrying", event, seq, url, status)
            elif row.attempts > len(RETRY_DELAYS_S):
                row.status = "dead"
                logger.error("webhook %s seq %d -> %s dead after %d attempts (last %s %s)", event, seq, url,
                             row.attempts, status, error or "")
            else:
                row.status = "pending"
                row.next_attempt_at = utcnow() + timedelta(seconds=RETRY_DELAYS_S[row.attempts - 1])
                logger.warning("webhook %s seq %d -> %s attempt %d failed (%s %s); retrying", event, seq, url,
                               row.attempts, status, error or "")
            await s.commit()
        if status is not None and 200 <= status < 300:
            self.wake()     # the next seq of this call is now eligible
