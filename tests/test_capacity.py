"""Capacity: engine pool from worker heartbeats, carrier pool per provider, CPS budget, inbound busy line."""

from fastapi.testclient import TestClient

from engine.api.app import build_app
from engine.api.capacity import CpsBudget, parse_limits
from engine.config import Settings
from conftest import H, HI, INTERNAL, KEY, FakePlivo, _agent_body

BODY = {"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                                         "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}


def _hb(client, **kw):
    body = {"worker_id": "w1", "worker_url": "http://w1:8091", "max_sessions": 20, "active_sessions": 0}
    body.update(kw)
    assert client.post("/internal/workers/heartbeat", json=body, headers=HI).status_code == 200


def _client_with(tmp_path, **kw):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'cap.db'}", engine_api_key=KEY,
                        internal_token=INTERNAL, plivo_auth_id="x", plivo_auth_token="y",
                        plivo_from_number="+918000000000", **kw)
    app = build_app(settings)
    c = TestClient(app)
    c.__enter__()
    app.state.engine.carriers._instances["plivo"] = FakePlivo(settings)
    return c


def test_parse_limits():
    assert parse_limits("plivo=50, vobiz=10") == {"plivo": 50, "vobiz": 10}
    assert parse_limits("") == {}


def test_cps_budget_allows_rate_per_second():
    b = CpsBudget(lambda p: 2)
    assert [b.try_acquire("plivo", now=100.0) for _ in range(3)] == [True, True, False]
    assert b.try_acquire("plivo", now=100.5) is True       # 0.5 s at 2/s refills exactly one token
    assert b.try_acquire("plivo", now=100.5) is False
    assert b.try_acquire("vobiz", now=100.5) is True        # providers have independent buckets


def test_engine_full_returns_429_reason(client):
    _hb(client, max_sessions=1, active_sessions=1)
    r = client.post("/v1/calls", json=BODY, headers=H)
    assert r.status_code == 429
    assert r.json()["detail"]["reason"] == "engine_full"
    assert r.headers["retry-after"] == "1"


def test_ringing_does_not_consume_engine_slot(client):
    _hb(client, max_sessions=1, active_sessions=0)
    assert client.post("/v1/calls", json=BODY, headers=H).status_code == 201     # dialing, not in progress
    assert client.post("/v1/calls", json=BODY, headers=H).status_code == 201


def test_in_progress_consumes_engine_slot(client):
    _hb(client, max_sessions=1, active_sessions=0)
    call_id = client.post("/v1/calls", json=BODY, headers=H).json()["call_id"]
    client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-1")
    r = client.post("/v1/calls", json=BODY, headers=H)
    assert r.status_code == 429 and r.json()["detail"]["reason"] == "engine_full"


def test_carrier_full_and_cps(tmp_path):
    c = _client_with(tmp_path, carrier_max_concurrent="plivo=1", carrier_cps="plivo=5")
    try:
        assert c.post("/v1/calls", json=BODY, headers=H).status_code == 201
        r = c.post("/v1/calls", json=BODY, headers=H)
        assert r.status_code == 429 and r.json()["detail"]["reason"] == "carrier_full"
    finally:
        c.__exit__(None, None, None)
    c = _client_with(tmp_path / "b", carrier_max_concurrent="plivo=5", carrier_cps="plivo=1")
    try:
        assert c.post("/v1/calls", json=BODY, headers=H).status_code == 201
        r = c.post("/v1/calls", json=BODY, headers=H)
        assert r.status_code == 429 and r.json()["detail"]["reason"] == "cps_exceeded"
    finally:
        c.__exit__(None, None, None)


def test_capacity_shape(client):
    _hb(client, max_sessions=5, active_sessions=2)
    cap = client.get("/v1/capacity", headers=H).json()
    assert cap["engine"] == {"free": 3, "total": 5}
    assert cap["carriers"]["plivo"]["total"] == 50 and cap["carriers"]["plivo"]["free"] == 50
    assert cap["carriers"]["plivo"]["cps_free"] > 0          # the shared fixture raises CARRIER_CPS; see _client_with
    assert cap["workers"][0]["worker_id"] == "w1"
    for legacy in ("active_calls", "max_concurrent_calls", "available", "accepting"):
        assert legacy in cap
    assert cap["available"] == 3 and cap["accepting"] is True


def test_capacity_legacy_without_workers(client):
    cap = client.get("/v1/capacity", headers=H).json()
    assert cap["engine"]["total"] == 20 and cap["engine"]["free"] == 20


def test_inbound_busy_when_full(client):
    _hb(client, max_sessions=1, active_sessions=1)
    client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    client.put("/v1/numbers/+918000000000", json={"provider": "plivo", "agent_id": "eo-rsvp"}, headers=H)
    xml = client.get("/carrier/plivo/answer?CallUUID=in-1&From=%2B919999999999&To=%2B918000000000&Direction=inbound")
    assert "<Speak" in xml.text and "<Hangup" in xml.text and "<Stream" not in xml.text
    calls = client.get("/v1/calls", headers=H).json()
    assert calls[0]["direction"] == "inbound" and calls[0]["status"] == "failed" and calls[0]["end_reason"] == "capacity"
