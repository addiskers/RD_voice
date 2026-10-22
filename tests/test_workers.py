"""Worker registry: heartbeats, dead-worker sweep, refused events, drain proxy."""

from datetime import datetime, timedelta, timezone

import httpx

from test_api import H, HI, FakePlivo, client  # noqa: F401  (fixture re-export)

BODY = {"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                                         "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}


def _hb(client, **kw):
    body = {"worker_id": "w1", "worker_url": "http://w1:8091", "max_sessions": 20, "active_sessions": 0}
    body.update(kw)
    return client.post("/internal/workers/heartbeat", json=body, headers=HI)


def _live_call_on_w1(client, uuid="cu-1"):
    call_id = client.post("/v1/calls", json=BODY, headers=H).json()["call_id"]
    client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID={uuid}")
    r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "started", "seq": 1, "data": {"worker_url": "http://w1:8091", "worker_id": "w1"}})
    assert r.status_code == 200
    return call_id


def _backdate_and_sweep(client, seconds=60):
    engine = client.app.state.engine

    async def go():
        from engine.api.db import Worker
        async with engine.db.session() as s:
            w = await s.get(Worker, "w1")
            w.last_seen_at = datetime.now(timezone.utc) - timedelta(seconds=seconds)
            await s.commit()
        async with engine.db.session() as s:
            return await engine.service.sweep_workers(s)

    return client.portal.call(go)


def test_heartbeat_upserts_and_lists(client):
    assert _hb(client).status_code == 200
    ws = client.get("/v1/workers", headers=H).json()
    assert ws[0]["worker_id"] == "w1" and ws[0]["status"] == "live" and ws[0]["seconds_since_seen"] < 5
    assert ws[0]["max_sessions"] == 20 and ws[0]["draining"] is False


def test_heartbeat_updates_max_sessions(client):
    _hb(client, max_sessions=20)
    _hb(client, max_sessions=8)
    ws = client.get("/v1/workers", headers=H).json()
    assert len(ws) == 1 and ws[0]["max_sessions"] == 8


def test_dead_worker_fails_its_calls_and_hangs_up(client):
    _hb(client)
    call_id = _live_call_on_w1(client)
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "in_progress"
    assert _backdate_and_sweep(client) == 1
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "worker_lost"
    assert FakePlivo.hangups == ["cu-1"]
    assert client.get("/v1/workers", headers=H).json()[0]["status"] == "dead"


def test_late_ended_after_worker_lost_is_ignored(client):
    _hb(client)
    call_id = _live_call_on_w1(client)
    _backdate_and_sweep(client)
    r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "ended", "seq": 2, "data": {"reason": "workflow_end", "duration_s": 12.0,
                          "transcript": [{"role": "agent", "text": "Hello", "at": 1.0},
                                         {"role": "user", "text": "Yes", "at": 2.0}]}})
    assert r.status_code == 200
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "worker_lost"
    assert len(rec["transcript"]) == 2


def test_refused_event_fails_call(client):
    _hb(client)
    call_id = _live_call_on_w1(client, uuid="cu-7")
    r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "refused", "seq": 2, "data": {"reason": "capacity", "worker_id": "w1"}})
    assert r.status_code == 200
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "capacity"
    assert FakePlivo.hangups[-1] == "cu-7"


def test_drain_proxies_to_worker(client, monkeypatch):
    _hb(client)
    seen = {}

    async def fake_post(self, url, **kw):
        seen["url"] = url
        seen["token"] = (kw.get("headers") or {}).get("X-Internal-Token")
        return httpx.Response(200, json={"draining": True, "active": 0})

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    r = client.post("/v1/workers/w1/drain", headers=H)
    assert r.status_code == 200 and r.json()["draining"] is True
    assert seen["url"] == "http://w1:8091/internal/drain" and seen["token"] == "test-internal"
    assert client.get("/v1/workers", headers=H).json()[0]["draining"] is True
    assert client.post("/v1/workers/nope/drain", headers=H).status_code == 404
