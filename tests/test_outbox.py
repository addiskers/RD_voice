"""Database-backed webhook outbox: per-call ordering, retries, dead-letter, replay, restart survival."""

import asyncio
import json
import time

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select

import engine.api.webhooks as wh
from engine.api.db import Call, Database, WebhookDelivery
from engine.api.webhooks import OutboxDispatcher, verify
from conftest import H  # noqa: F401

SECRET = "s3"


@pytest_asyncio.fixture
async def tmp_db(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    await db.init()
    yield db
    await db.close()


async def _call(db: Database, call_id: str, url: str = "https://platform/hook") -> None:
    async with db.session() as s:
        s.add(Call(id=call_id, status="in_progress", direction="outbound", provider="plivo", to_number="+91",
                   webhook_url=url, webhook_secret=SECRET))
        await s.commit()


async def _enqueue(db: Database, disp: OutboxDispatcher, call_id: str, etype: str, data: dict) -> WebhookDelivery:
    async with db.session() as s:
        call = await s.get(Call, call_id)
        return await disp.enqueue(s, call, etype, data)


async def _rows(db: Database, call_id: str) -> list[WebhookDelivery]:
    async with db.session() as s:
        return list(await s.scalars(select(WebhookDelivery).where(WebhookDelivery.call_id == call_id)
                                    .order_by(WebhookDelivery.seq)))


async def _pump(disp: OutboxDispatcher, passes: int = 12) -> None:
    for _ in range(passes):
        await disp.deliver_due()
        await asyncio.sleep(0.01)


def _dispatcher(db: Database, handler) -> OutboxDispatcher:
    return OutboxDispatcher(db, http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


@pytest.mark.asyncio
async def test_enqueue_allocates_seq_per_call(tmp_db):
    await _call(tmp_db, "call_a"); await _call(tmp_db, "call_b")
    disp = _dispatcher(tmp_db, lambda r: httpx.Response(200))
    for i in range(3):
        await _enqueue(tmp_db, disp, "call_a", "call.turn", {"i": i})
    await _enqueue(tmp_db, disp, "call_b", "call.turn", {"i": 0})
    assert [r.seq for r in await _rows(tmp_db, "call_a")] == [1, 2, 3]
    assert [r.seq for r in await _rows(tmp_db, "call_b")] == [1]
    async with tmp_db.session() as s:
        assert (await s.get(Call, "call_a")).webhook_seq == 3


@pytest.mark.asyncio
async def test_delivers_in_order_and_retries(tmp_db, monkeypatch):
    monkeypatch.setattr(wh, "RETRY_DELAYS_S", (0.0, 0.0))
    await _call(tmp_db, "call_a")
    received, attempts = [], {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        attempts["n"] += 1
        if body["seq"] == 2 and attempts["n"] == 2:
            return httpx.Response(500)
        assert verify(SECRET, request.headers["x-gvox-timestamp"], request.content, request.headers["x-gvox-signature"])
        received.append(body["seq"])
        return httpx.Response(200)

    disp = _dispatcher(tmp_db, handler)
    for i in range(3):
        await _enqueue(tmp_db, disp, "call_a", "call.turn" if i < 2 else "call.ended", {"i": i})
    await _pump(disp)
    assert received == [1, 2, 3]
    rows = await _rows(tmp_db, "call_a")
    assert [r.status for r in rows] == ["delivered"] * 3 and rows[1].attempts == 2 and rows[1].delivered_at is not None


@pytest.mark.asyncio
async def test_4xx_is_dead_immediately_except_408_429(tmp_db):
    await _call(tmp_db, "call_a"); await _call(tmp_db, "call_b")
    disp = _dispatcher(tmp_db, lambda r: httpx.Response(404 if "call_a" in r.content.decode() else 429))
    await _enqueue(tmp_db, disp, "call_a", "call.turn", {})
    await _enqueue(tmp_db, disp, "call_b", "call.turn", {})
    await disp.deliver_due()
    a, b = (await _rows(tmp_db, "call_a"))[0], (await _rows(tmp_db, "call_b"))[0]
    assert a.status == "dead" and a.attempts == 1 and a.last_status_code == 404
    assert b.status == "pending" and b.attempts == 1 and b.last_status_code == 429


@pytest.mark.asyncio
async def test_dead_after_final_attempt_and_replay(tmp_db, monkeypatch):
    monkeypatch.setattr(wh, "RETRY_DELAYS_S", (0.0, 0.0))
    await _call(tmp_db, "call_a")
    mode = {"code": 500}
    disp = _dispatcher(tmp_db, lambda r: httpx.Response(mode["code"]))
    await _enqueue(tmp_db, disp, "call_a", "call.turn", {})
    await _pump(disp)
    row = (await _rows(tmp_db, "call_a"))[0]
    assert row.status == "dead" and row.attempts == 3
    mode["code"] = 200
    async with tmp_db.session() as s:
        assert await disp.replay(s, "call_a", 1) is True
        assert await disp.replay(s, "call_a", 9) is False
    await _pump(disp)
    assert (await _rows(tmp_db, "call_a"))[0].status == "delivered"


@pytest.mark.asyncio
async def test_survives_restart(tmp_db):
    await _call(tmp_db, "call_a")
    first = _dispatcher(tmp_db, lambda r: httpx.Response(500))     # never runs a pass: rows stay pending
    await _enqueue(tmp_db, first, "call_a", "call.turn", {"i": 1})
    await _enqueue(tmp_db, first, "call_a", "call.ended", {"i": 2})
    received = []

    def handler(r):
        received.append(json.loads(r.content)["seq"])
        return httpx.Response(200)

    second = _dispatcher(tmp_db, handler)
    await _pump(second, passes=4)
    assert received == [1, 2]


@pytest.mark.asyncio
async def test_outbox_unicode_payload_signature_verifies(tmp_db):
    await _call(tmp_db, "call_a")
    text = "हाँ, मैं आ रहा हूँ"
    seen = {}

    def handler(r):
        seen["ok"] = verify(SECRET, r.headers["x-gvox-timestamp"], r.content, r.headers["x-gvox-signature"])
        seen["text"] = json.loads(r.content)["data"]["text"]
        return httpx.Response(200)

    disp = _dispatcher(tmp_db, handler)
    await _enqueue(tmp_db, disp, "call_a", "call.turn", {"text": text})
    await _pump(disp, passes=2)
    assert seen == {"ok": True, "text": text}


def test_deliveries_endpoints(client):
    received = []

    def handler(r):
        received.append(json.loads(r.content)["type"])
        return httpx.Response(200)

    client.app.state.engine.dispatcher._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    body = {"to": "+919876543210", "webhook_url": "https://platform/hook",
            "agent": {"name": "s", "instructions": "x", "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}
    call_id = client.post("/v1/calls", json=body, headers=H).json()["call_id"]
    for _ in range(40):
        rows = client.get(f"/v1/calls/{call_id}/deliveries", headers=H).json()
        if rows and all(r["status"] == "delivered" for r in rows):
            break
        time.sleep(0.1)
    assert [r["event"] for r in rows] == ["call.queued"] and rows[0]["seq"] == 1 and rows[0]["attempts"] == 1
    assert client.post(f"/v1/calls/{call_id}/deliveries/1/replay", headers=H).status_code == 200
    for _ in range(40):
        rows = client.get(f"/v1/calls/{call_id}/deliveries", headers=H).json()
        if rows[0]["attempts"] == 2 and rows[0]["status"] == "delivered":
            break
        time.sleep(0.1)
    assert rows[0]["attempts"] == 2 and received == ["call.queued", "call.queued"]
    assert client.post(f"/v1/calls/{call_id}/deliveries/7/replay", headers=H).status_code == 404
