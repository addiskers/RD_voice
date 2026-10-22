"""The `fake` carrier (test-only) and the Plivo-protocol client script that drives the real worker endpoint."""

import base64
import json
import os
import sys

from fastapi.testclient import TestClient

from conftest import H, INTERNAL, KEY
from engine.api.app import build_app
from engine.config import Settings
from engine.worker import stream_protocol as sp

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
from fake_carrier import media_frame, start_frame, stop_frame  # noqa: E402

BODY = {"to": "+910000000001", "provider": "fake", "agent": {"name": "s", "instructions": "x",
                                                            "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}


def _client(tmp_path, **kw) -> TestClient:
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'fake.db'}", engine_api_key=KEY,
                        internal_token=INTERNAL, public_http_url="https://api.test", public_ws_url="wss://media.test", **kw)
    c = TestClient(build_app(settings))
    c.__enter__()
    return c


def test_fake_dial_answer_and_hangup(tmp_path):
    c = _client(tmp_path, enable_test_endpoints=True, carrier_verify_signatures=True)
    try:
        r = c.post("/v1/calls", json=BODY, headers=H)
        assert r.status_code == 201 and r.json()["status"] == "dialing"
        call_id, pcid = r.json()["call_id"], r.json()["provider_call_id"]
        assert pcid.startswith("fake-")
        xml = c.get(f"/carrier/fake/answer?call_id={call_id}&CallUUID={pcid}&From=%2B910000000000&To=%2B910000000001")
        assert xml.status_code == 200 and "<Stream" in xml.text          # no signature needed for the fake carrier
        assert c.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "in_progress"
        r = c.post(f"/carrier/fake/hangup?call_id={call_id}", data={"CallUUID": pcid, "HangupCauseCode": "4000",
                                                                    "Duration": "12", "AnswerTime": "2026-10-23 10:00:00"})
        assert r.status_code == 200
        rec = c.get(f"/v1/calls/{call_id}", headers=H).json()
        assert rec["status"] == "completed" and rec["duration_s"] == 12.0
        cap = c.get("/v1/capacity", headers=H).json()
        assert cap["carriers"]["fake"]["total"] == 1000
    finally:
        c.__exit__(None, None, None)


def test_fake_not_configured_by_default(client):
    assert client.post("/v1/calls", json=BODY, headers=H).status_code == 503


def test_fake_carrier_frames():
    msg = json.loads(start_frame("st-1", "fake-abc", {"X-Call-Id": "call_1", "X-Test-Token": "t"}))
    start = sp.parse_start(msg)
    assert start.stream_id == "st-1" and start.call_id == "fake-abc" and start.sample_rate == 8000
    assert start.extra_headers == {"X-Call-Id": "call_1", "X-Test-Token": "t"}
    media = json.loads(media_frame(b"\xff" * 160))
    assert sp.event_type(media) == "media" and len(base64.b64decode(sp.media_payload(media))) == 160
    assert sp.event_type(json.loads(stop_frame("st-1"))) == "stop"


def test_fake_carrier_rejected_without_test_token():
    from engine.worker.main import should_reject_fake
    s = Settings(_env_file=None, internal_token="tok")
    assert should_reject_fake({"provider": "fake"}, {"x-test-token": "wrong"}, s) is True
    assert should_reject_fake({"provider": "fake"}, {}, s) is True
    assert should_reject_fake({"provider": "fake"}, {"x-test-token": "tok"}, s) is False
    assert should_reject_fake({"provider": "plivo"}, {}, s) is False
