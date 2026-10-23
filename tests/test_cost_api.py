"""The API turns the worker's usage_detail into a stored cost breakdown and can recompute it on demand."""

import pytest
from conftest import HI, H
from test_workers import _live_call_on_w1

USAGE = {
    "stt": [{"provider": "deepgram", "model": "flux-general-multi", "seconds": 60.0, "recognized_seconds": 12.5}],
    "llm": [{"provider": "google", "model": "gemini-2.5-flash-lite", "requests": 4, "input_tokens": 8000,
             "cached_input_tokens": 0, "output_tokens": 200}],
    "tts": [{"provider": "cartesia", "model": "sonic-3", "requests": 3, "characters": 300, "cancelled_characters": 0,
             "audio_seconds": 18.0}],
}


def test_ended_event_stores_cost_breakdown(client):
    call_id = _live_call_on_w1(client, uuid="cu-cost")
    r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "ended", "seq": 2, "data": {"reason": "workflow_end", "duration_s": 60.0,
                                                               "usage_detail": USAGE}})
    assert r.status_code == 200
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    cost = rec["metrics"]["cost"]
    stt = cost["stages"]["stt"]["lines"][0]
    llm = cost["stages"]["llm"]["lines"][0]
    tts = cost["stages"]["tts"]["lines"][0]
    assert stt["quantity"] == 1.0 and stt["usd"] == pytest.approx(0.0078)
    assert llm["usd"] == pytest.approx((8000 * 0.10 + 200 * 0.40) / 1e6)
    assert tts["usd"] == pytest.approx(300 * 0.000038)
    assert cost["stages"]["carrier"]["lines"][0]["quantity"] == 1.0           # 60 s = two 30 s pulses
    assert cost["stages"]["carrier"]["lines"][0]["inr"] == pytest.approx(0.38)
    assert rec["cost_usd"] == pytest.approx(cost["total_usd"]) and cost["incomplete"] is False
    assert cost["models_usd"] == pytest.approx(stt["usd"] + llm["usd"] + tts["usd"])
    assert rec["metrics"]["usage_detail"] == USAGE


def test_cost_endpoint_recomputes_and_404s_without_usage(client):
    call_id = _live_call_on_w1(client, uuid="cu-cost2")
    assert client.get(f"/v1/calls/{call_id}/cost", headers=H).status_code == 404
    client.post(f"/internal/calls/{call_id}/events", headers=HI,
                json={"type": "ended", "seq": 2, "data": {"reason": "workflow_end", "duration_s": 45.0, "usage_detail": USAGE}})
    cost = client.get(f"/v1/calls/{call_id}/cost", headers=H).json()
    assert cost["total_usd"] > 0 and cost["as_of"] and "estimate" in cost["disclaimer"]
    assert cost["stages"]["carrier"]["lines"][0]["quantity"] == 1.0            # 45 s -> 2 pulses -> 1.0 min


def test_failover_events_survive_the_final_metrics_snapshot(client):
    # the worker posts "provider" events as failovers happen and a full metrics snapshot on "ended";
    # the record must keep both, or the Calls tab shows "failover: none" on a call that failed over
    call_id = _live_call_on_w1(client, uuid="cu-prov")
    client.post(f"/internal/calls/{call_id}/events", headers=HI,
                json={"type": "provider", "seq": 2, "data": {"kind": "tts", "provider": "cartesia", "available": False, "at": 3.2}})
    client.post(f"/internal/calls/{call_id}/events", headers=HI,
                json={"type": "metrics", "seq": 3, "data": {"turns": [], "models": {"tts": "cartesia sonic-3"}}})
    client.post(f"/internal/calls/{call_id}/events", headers=HI,
                json={"type": "ended", "seq": 4, "data": {"reason": "workflow_end", "duration_s": 20.0, "usage_detail": USAGE,
                                                           "metrics": {"turns": [], "models": {"tts": "cartesia sonic-3"}}}})
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["metrics"]["provider_events"] == [{"kind": "tts", "provider": "cartesia", "available": False, "at": 3.2}]
    assert rec["metrics"]["cost"]["total_usd"] > 0


def test_realtime_call_with_no_model_usage_still_gets_a_carrier_cost(client):
    # a speech-to-speech caller who hangs up in the greeting produces no model metrics, but the carrier billed a pulse
    call_id = _live_call_on_w1(client, uuid="cu-empty")
    r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                    json={"type": "ended", "seq": 2, "data": {"reason": "carrier_hangup", "duration_s": 12.0, "usage_detail": {}}})
    assert r.status_code == 200
    cost = client.get(f"/v1/calls/{call_id}/cost", headers=H).json()
    assert list(cost["stages"]) == ["carrier"] and cost["incomplete"] is False


def test_a_broken_price_list_never_blocks_call_finalization(client):
    svc = client.app.state.engine.service
    svc.prices.models["google/gemini-2.5-flash-lite"] = {"stage": "llm", "unit": "token"}     # missing rates
    try:
        call_id = _live_call_on_w1(client, uuid="cu-broken")
        r = client.post(f"/internal/calls/{call_id}/events", headers=HI,
                        json={"type": "ended", "seq": 2, "data": {"reason": "workflow_end", "duration_s": 30.0, "usage_detail": USAGE}})
        assert r.status_code == 200
        rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
        assert rec["status"] == "completed"
        assert rec["metrics"]["cost"]["incomplete"] is True
        assert any("pricing error" in n for n in rec["metrics"]["cost"]["notes"])
    finally:
        svc.prices.models.pop("google/gemini-2.5-flash-lite", None)
