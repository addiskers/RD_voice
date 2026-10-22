"""Regression tests for the whole-branch review findings (C1–C5, I1–I8)."""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from cryptography.fernet import Fernet
from sqlalchemy.exc import IntegrityError

from conftest import H, HI, INTERNAL, FakePlivo, _agent_body
from engine.api.db import Call, WebhookDelivery
from engine.api.webhooks import OutboxDispatcher
from test_capacity import BODY as CAP_BODY
from test_capacity import _hb as _hb_cap
from test_outbox import SECRET, _call, _dispatcher, _enqueue, _pump, _rows, tmp_db  # noqa: F401
from test_recording_storage import _insert_call_with_recording
from test_security import _client as _sec_client
from test_workers import _hb, _live_call_on_w1


# ----------------------------------------------------------------------------- C1 / I2 / I3: outbox
@pytest.mark.asyncio
async def test_restart_resets_delivering_rows(tmp_db):
    """C1: a row left 'delivering' by a crash mid-send must not block the call forever."""
    await _call(tmp_db, "call_a")
    first = _dispatcher(tmp_db, lambda r: httpx.Response(500))
    await _enqueue(tmp_db, first, "call_a", "call.turn", {"i": 1})
    await _enqueue(tmp_db, first, "call_a", "call.ended", {"i": 2})
    async with tmp_db.session() as s:
        row = await s.get(WebhookDelivery, (await _rows(tmp_db, "call_a"))[0].id)
        row.status = "delivering"
        await s.commit()
    received = []

    def handler(r):
        received.append(json.loads(r.content)["seq"])
        return httpx.Response(200)

    second = _dispatcher(tmp_db, handler)
    await second.start()
    try:
        await _pump(second, passes=4)
    finally:
        await second.close()
    assert received == [1, 2]


@pytest.mark.asyncio
async def test_enqueue_does_not_commit_on_its_own(tmp_db):
    """I2: the outbox row is committed together with the state change by the caller."""
    await _call(tmp_db, "call_a")
    disp = _dispatcher(tmp_db, lambda r: httpx.Response(200))
    async with tmp_db.session() as s:
        call = await s.get(Call, "call_a")
        await disp.enqueue(s, call, "call.turn", {})
        await s.rollback()
    assert await _rows(tmp_db, "call_a") == []


@pytest.mark.asyncio
async def test_enqueue_seq_is_atomic_under_concurrency(tmp_db):
    """I2: two sessions enqueueing for the same call must not both take seq 1."""
    await _call(tmp_db, "call_a")
    disp = _dispatcher(tmp_db, lambda r: httpx.Response(200))

    async def one(etype):
        async with tmp_db.session() as s:
            call = await s.get(Call, "call_a")
            await asyncio.sleep(0.01)
            await disp.enqueue(s, call, etype, {})
            await s.commit()

    await asyncio.gather(one("call.turn"), one("call.ended"))
    assert sorted(r.seq for r in await _rows(tmp_db, "call_a")) == [1, 2]


@pytest.mark.asyncio
async def test_unique_index_on_call_seq(tmp_db):
    async with tmp_db.session() as s:
        s.add(WebhookDelivery(id="d1", call_id="call_x", seq=1, event="e", payload_json="{}", url="u"))
        s.add(WebhookDelivery(id="d2", call_id="call_x", seq=1, event="e", payload_json="{}", url="u"))
        with pytest.raises(IntegrityError):
            await s.commit()


@pytest.mark.asyncio
async def test_undecryptable_secret_marks_row_dead_and_pass_continues(tmp_db):
    """I3: one bad secret must not stop every other call's webhooks."""
    await _call(tmp_db, "call_bad")
    await _call(tmp_db, "call_good")
    received = []

    def handler(r):
        received.append(json.loads(r.content)["call_id"])
        return httpx.Response(200)

    def resolver(secret):
        if secret == SECRET:
            return secret
        raise ValueError("cannot decrypt")

    async with tmp_db.session() as s:
        (await s.get(Call, "call_bad")).webhook_secret = "enc:v1:garbage"
        await s.commit()
    disp = OutboxDispatcher(tmp_db, http=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                            secret_resolver=resolver)
    await _enqueue(tmp_db, disp, "call_bad", "call.turn", {})
    await _enqueue(tmp_db, disp, "call_good", "call.turn", {})
    await _pump(disp, passes=3)
    assert received == ["call_good"]
    bad = (await _rows(tmp_db, "call_bad"))[0]
    assert bad.status == "dead" and "decrypt" in (bad.last_error or "")


# ----------------------------------------------------------------------------- C4 / I1: workers
def test_worker_restart_finalizes_previous_incarnation_calls(client):
    """C4: a crashed worker restarted by Docker (same id, new started_at) heartbeats again within 20 s, so the
    dead-worker sweep never fires; the previous incarnation's calls must still be finalized."""
    _hb(client, started_at="2026-10-23T10:00:00+00:00")
    live = _live_call_on_w1(client, uuid="cu-a")
    hung = _live_call_on_w1(client, uuid="cu-b")
    client.post(f"/carrier/plivo/hangup?call_id={hung}", data={"CallUUID": "cu-b", "HangupCauseCode": "4000",
                                                               "AnswerTime": "2026-10-23 10:00:00", "Duration": "20"})
    _hb(client, started_at="2026-10-23T10:05:00+00:00", active_sessions=0)      # new incarnation
    rec = client.get(f"/v1/calls/{live}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "worker_lost"
    assert "cu-a" in FakePlivo.hangups
    rec = client.get(f"/v1/calls/{hung}", headers=H).json()
    assert rec["status"] == "completed" and rec["end_reason"] == "worker_lost"
    fresh = _live_call_on_w1(client, uuid="cu-c")
    _hb(client, started_at="2026-10-23T10:05:00+00:00", active_sessions=1)      # same incarnation: no-op
    assert client.get(f"/v1/calls/{fresh}", headers=H).json()["status"] == "in_progress"


def test_stale_sweep_finalizes_completed_without_terminal_event(client):
    """C4: a carrier-hung-up call whose worker never reports `ended` gets its terminal webhook eventually."""
    call_id = _live_call_on_w1(client, uuid="cu-s")
    client.post(f"/carrier/plivo/hangup?call_id={call_id}", data={"CallUUID": "cu-s", "HangupCauseCode": "4000",
                                                                  "AnswerTime": "2026-10-23 10:00:00", "Duration": "20"})
    engine = client.app.state.engine

    async def backdate_and_sweep():
        async with engine.db.session() as s:
            c = await s.get(Call, call_id)
            c.created_at = datetime.now(timezone.utc) - timedelta(seconds=600 + 120 + 5)
            await s.commit()
        async with engine.db.session() as s:
            return await engine.service.sweep_stale(s)

    assert client.portal.call(backdate_and_sweep) == 1

    async def emitted():
        async with engine.db.session() as s:
            return (await s.get(Call, call_id)).ended_emitted

    assert client.portal.call(emitted) is True
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "completed"


def test_stt_failed_reason_marks_call_failed(client):
    """I1: the worker ends a call with stt_failed (Flux died): that is a failed call, not a completed one."""
    call_id = _live_call_on_w1(client, uuid="cu-f")
    client.post(f"/internal/calls/{call_id}/events", headers=HI,
                json={"type": "ended", "seq": 2, "data": {"reason": "stt_failed", "duration_s": 3.0}})
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "stt_failed"


# ----------------------------------------------------------------------------- C3: capacity during drain
def test_draining_worker_calls_do_not_consume_new_worker_slots(client):
    """C3: during a blue/green drain the old worker's live calls must not be charged to the new worker."""
    _hb_cap(client, worker_id="w1", worker_url="http://w1:8091", max_sessions=20, active_sessions=3)
    for uuid in ("d1", "d2", "d3"):
        call_id = client.post("/v1/calls", json=CAP_BODY, headers=H).json()["call_id"]
        client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID={uuid}")
        client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "started", "seq": 1, "data": {"worker_url": "http://w1:8091", "worker_id": "w1"}})
    _hb_cap(client, worker_id="w1", worker_url="http://w1:8091", max_sessions=20, active_sessions=3, draining=True)
    _hb_cap(client, worker_id="w2", worker_url="http://w2:8091", max_sessions=2, active_sessions=0)
    cap = client.get("/v1/capacity", headers=H).json()
    assert cap["engine"] == {"free": 2, "total": 2}


# ----------------------------------------------------------------------------- I7: retention of local files
def test_retention_deletes_legacy_local_files(client, tmp_path):
    local = tmp_path / "legacy.wav"
    local.write_bytes(b"RIFF")
    engine = client.app.state.engine
    _insert_call_with_recording(client, str(local), days_old=100)

    async def sweep():
        async with engine.db.session() as s:
            return await engine.service.sweep_recordings(s, engine.storage)

    assert client.portal.call(sweep) == 1
    assert not local.exists()
    assert client.get("/v1/calls/call_rec", headers=H).json()["recording_url"] is None


# ----------------------------------------------------------------------------- C2 / I4: carrier routes, masked keys
def test_unconfigured_carrier_route_404(tmp_path):
    c = _sec_client(tmp_path, carrier_verify_signatures=True)
    try:
        assert c.get("/carrier/enablex/answer?call_id=x").status_code == 404
        assert c.post("/carrier/fake/hangup?call_id=x", data={"CallUUID": "cu"}).status_code == 404
    finally:
        c.__exit__(None, None, None)


def test_cross_provider_callback_rejected(tmp_path):
    """A plivo call's state cannot be driven through another provider's (unverified) route."""
    c = _sec_client(tmp_path, carrier_verify_signatures=True, enable_test_endpoints=True)
    try:
        call_id = c.post("/v1/calls", json={"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                         "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}, headers=H).json()["call_id"]
        r = c.post(f"/carrier/fake/hangup?call_id={call_id}", data={"CallUUID": "cu-1", "HangupCauseCode": "4000"})
        assert r.status_code == 404
        assert c.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "dialing"
    finally:
        c.__exit__(None, None, None)


def test_carrier_without_verifier_fails_closed(tmp_path):
    c = _sec_client(tmp_path, carrier_verify_signatures=True, enablex_app_id="a", enablex_app_key="k",
                    enablex_from_number="+910000000009")
    try:
        assert c.get("/carrier/enablex/answer?call_id=x").status_code == 403
    finally:
        c.__exit__(None, None, None)


def test_put_agent_with_masked_key_keeps_previous_key(tmp_path):
    key = Fernet.generate_key().decode()
    c = _sec_client(tmp_path, carrier_verify_signatures=False, engine_encryption_key=key)
    try:
        agent = _agent_body()
        agent["stt"] = {"provider": "deepgram", "model": "nova-3", "api_key": "dg-secret-key-1234"}
        assert c.put("/v1/agents/eo-rsvp", json=agent, headers=H).status_code == 200
        shown = c.get("/v1/agents/eo-rsvp", headers=H).json()["config"]
        assert shown["stt"]["api_key"] == "****1234"
        assert c.put("/v1/agents/eo-rsvp", json=shown, headers=H).status_code == 200      # round-trip the masked config
        call_id = c.post("/v1/calls", json={"to": "+919876543210", "agent": {"agent_id": "eo-rsvp"},
                                            "variables": {"first_name": "P"}}, headers=H).json()["call_id"]
        rt = c.get(f"/internal/calls/{call_id}/runtime", headers={"X-Internal-Token": INTERNAL}).json()
        assert rt["agent"]["stt"]["api_key"] == "dg-secret-key-1234"
        fresh = _agent_body()
        fresh["stt"] = {"provider": "deepgram", "model": "nova-3", "api_key": "****9999"}
        assert c.put("/v1/agents/never-seen", json=fresh, headers=H).status_code == 422
    finally:
        c.__exit__(None, None, None)
