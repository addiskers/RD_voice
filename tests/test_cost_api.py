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
