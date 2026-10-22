import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from engine.api.app import build_app
from engine.carriers.base import Carrier, DialResult
from engine.config import Settings

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
KEY = "test-key"
INTERNAL = "test-internal"


class FakePlivo(Carrier):
    name = "plivo"
    dials: list = []

    @property
    def from_number(self):
        return "+918000000000"

    def configured(self):
        return True

    async def dial(self, to, from_number, *, answer_url, ring_url, hangup_url, stream_url, extra=None):
        self.dials.append({"to": to, "answer_url": answer_url, "hangup_url": hangup_url, "stream_url": stream_url})
        return DialResult(provider_call_id="rq-1", raw={})

    async def hangup(self, provider_call_id):
        return True


@pytest.fixture
def client(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'engine.db'}",
                        recordings_dir=str(tmp_path / "rec"), engine_api_key=KEY, internal_token=INTERNAL,
                        public_http_url="https://api.test", public_ws_url="wss://media.test",
                        plivo_auth_id="x", plivo_auth_token="y", plivo_from_number="+918000000000")
    app = build_app(settings)
    with TestClient(app) as c:
        engine = app.state.engine
        engine.carriers._instances["plivo"] = FakePlivo(settings)
        FakePlivo.dials.clear()
        yield c


def _agent_body():
    agent = json.loads((EXAMPLES / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
    agent["workflow"] = json.loads((EXAMPLES / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))
    return agent


H = {"Authorization": f"Bearer {KEY}"}
HI = {"X-Internal-Token": INTERNAL}


def test_auth_required(client):
    assert client.get("/v1/capacity").status_code == 401
    assert client.get("/health").status_code == 200


def test_agent_versions_and_validate(client):
    r = client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    assert r.status_code == 200 and r.json()["version"] == 1
    r = client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    assert r.json()["version"] == 2
    assert client.get("/v1/agents/eo-rsvp", headers=H).json()["version"] == 2
    assert client.get("/v1/agents/eo-rsvp?version=1", headers=H).json()["version"] == 1
    v = client.post("/v1/agents/validate", json=_agent_body(), headers=H).json()
    assert v["ok"] and "first_name" in v["required_variables"]
    bad = _agent_body()
    bad["workflow"]["nodes"].append({"id": "s2", "type": "startCall", "data": {"name": "dup", "prompt": "p"}})
    assert client.put("/v1/agents/bad", json=bad, headers=H).status_code == 422


def test_outbound_call_lifecycle(client):
    client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    r = client.post("/v1/calls", json={"to": "+919876543210", "agent": {"agent_id": "eo-rsvp"},
                                       "variables": {"first_name": "Pratik"}, "metadata": {"contact": 7}}, headers=H)
    assert r.status_code == 201, r.text
    call_id = r.json()["call_id"]
    assert r.json()["status"] == "dialing"
    assert FakePlivo.dials[0]["answer_url"] == f"https://api.test/carrier/plivo/answer?call_id={call_id}"
    assert FakePlivo.dials[0]["stream_url"] == "wss://media.test"

    cap = client.get("/v1/capacity", headers=H).json()
    assert cap["active_calls"] == 1

    # ring + answer callbacks from the carrier
    assert client.post(f"/carrier/plivo/ring?call_id={call_id}", data={"CallUUID": "cu-1", "CallStatus": "ringing"}).status_code == 200
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "ringing"
    xml = client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-1&From=%2B918000000000&To=%2B919876543210")
    assert xml.status_code == 200 and "wss://media.test" in xml.text and "X-Call-Id=" in xml.text
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "in_progress" and rec["provider_call_id"] == "cu-1"

    # worker resolves the call by the carrier uuid, then reports
    rt = client.get("/internal/calls/by-provider/cu-1/runtime", headers=HI).json()
    assert rt["call_id"] == call_id and rt["agent"]["name"].startswith("EO") and rt["variables"] == {"first_name": "Pratik"}
    ev = lambda t, d, s: client.post(f"/internal/calls/{call_id}/events", json={"type": t, "seq": s, "data": d}, headers=HI)
    assert ev("started", {"worker_url": "http://w:8091"}, 1).status_code == 200
    ev("turn", {"role": "agent", "text": "Hello! Am I speaking to Pratik?", "at": 1.2}, 2)
    ev("turn", {"role": "user", "text": "Yes", "at": 3.0}, 3)
    ev("tool", {"name": "record_outcome", "record_outcome": True, "result": {"status": "yes", "kids_count": 2}}, 4)
    ev("ended", {"reason": "workflow_end", "duration_s": 42.0, "hangup_carrier": True,
                 "transcript": [{"role": "agent", "text": "Hello!", "at": 1.2}, {"role": "user", "text": "Yes", "at": 3.0}],
                 "metrics": {"turns": [{"turn": 1, "eou_delay": 0.4, "llm_ttft": 0.3, "tts_ttfb": 0.1, "total": 0.8}],
                             "total_p50": 0.8}}, 5)
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "completed" and rec["end_reason"] == "workflow_end"
    assert rec["outcome"]["status"] == "yes" and rec["outcome"]["kids_count"] == 2
    assert len(rec["transcript"]) == 2 and rec["metrics"]["total_p50"] == 0.8
    assert rec["duration_s"] == 42.0
    # late carrier hangup must not change the terminal state
    client.post(f"/carrier/plivo/hangup?call_id={call_id}", data={"CallUUID": "cu-1", "HangupCauseCode": "4000"})
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "completed"
    assert client.get("/v1/capacity", headers=H).json()["active_calls"] == 0


def test_no_answer_and_busy(client):
    client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    for code, status in (("3000", "no_answer"), ("3010", "busy")):
        call_id = client.post("/v1/calls", json={"to": "+919876543210", "agent": {"agent_id": "eo-rsvp"}},
                              headers=H).json()["call_id"]
        client.post(f"/carrier/plivo/hangup?call_id={call_id}", data={"CallUUID": "cu-x", "HangupCauseCode": code})
        rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
        assert rec["status"] == status and rec["end_reason"] == "carrier_hangup"


def test_inbound_routing_and_unknown_number(client):
    client.put("/v1/agents/eo-rsvp", json=_agent_body(), headers=H)
    r = client.put("/v1/numbers/+918000000000", json={"provider": "plivo", "agent_id": "eo-rsvp"}, headers=H)
    assert r.status_code == 200 and r.json()["carrier_application"]["answer_url"].endswith("/carrier/plivo/answer")
    xml = client.get("/carrier/plivo/answer?CallUUID=in-1&From=%2B919999999999&To=%2B918000000000&Direction=inbound")
    assert "<Stream" in xml.text
    calls = client.get("/v1/calls", headers=H).json()
    assert calls[0]["direction"] == "inbound" and calls[0]["status"] == "in_progress"
    assert calls[0]["variables"]["caller"] == "+919999999999"
    hang = client.get("/carrier/plivo/answer?CallUUID=in-2&From=%2B919999999999&To=%2B917000000000&Direction=inbound")
    assert "<Hangup" in hang.text


def test_inline_agent_and_hangup_api(client):
    body = {"to": "+919876543210", "agent": {"name": "simple", "instructions": "Be brief.",
                                             "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}
    call_id = client.post("/v1/calls", json=body, headers=H).json()["call_id"]
    r = client.post(f"/v1/calls/{call_id}/hangup", headers=H)
    assert r.status_code == 200
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "cancelled"


def test_stale_sweep(client):
    import asyncio
    from datetime import datetime, timedelta, timezone
    body = {"to": "+919876543210", "agent": {"name": "s", "instructions": "x", "max_duration_s": 60,
                                             "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}
    call_id = client.post("/v1/calls", json=body, headers=H).json()["call_id"]
    client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-9")
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "in_progress"
    engine = client.app.state.engine

    async def backdate_and_sweep():
        async with engine.db.session() as s:
            from engine.api.db import Call
            c = await s.get(Call, call_id)
            c.created_at = datetime.now(timezone.utc) - timedelta(seconds=60 + 120 + 5)
            await s.commit()
        async with engine.db.session() as s:
            return await engine.service.sweep_stale(s)

    n = client.portal.call(backdate_and_sweep)
    assert n == 1
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "stale_timeout"


def test_capacity_limit(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'c.db'}", engine_api_key=KEY,
                        internal_token=INTERNAL, max_concurrent_calls=1, plivo_auth_id="x", plivo_auth_token="y",
                        plivo_from_number="+918000000000")
    app = build_app(settings)
    with TestClient(app) as c:
        app.state.engine.carriers._instances["plivo"] = FakePlivo(settings)
        body = {"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                                                 "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}
        assert c.post("/v1/calls", json=body, headers=H).status_code == 201
        assert c.post("/v1/calls", json=body, headers=H).status_code == 429
        assert c.get("/v1/capacity", headers=H).json()["accepting"] is False
