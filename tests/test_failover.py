"""STT/LLM fallback adapters, the Flux circuit breaker, and per-turn provider attribution."""

import pytest
from livekit.agents import llm as lk_llm, stt as lk_stt
from livekit.plugins import deepgram

from engine.config import Settings
from engine.schemas.agent import AgentConfig, Mode, ProviderRef, VoiceRef
from engine.schemas.calls import TurnMetrics
from engine.worker.breaker import BREAKER, ProviderBreaker
from engine.worker.pipeline import build_llm, build_stt, make_session, provider_id, provider_labels
from engine.worker.tools import CallState
from conftest import H, HI

S = Settings(_env_file=None, deepgram_api_key="d", sarvam_api_key="s", google_api_key="g", groq_api_key="q",
             cartesia_api_key="c", openai_api_key="o")


def _cfg(stt_model: str) -> AgentConfig:
    cfg = AgentConfig(name="a", mode=Mode.cascaded, instructions="x",
                      stt=ProviderRef(provider="deepgram", model=stt_model),
                      llm=ProviderRef(provider="google", model="gemini-2.5-flash-lite"),
                      tts=VoiceRef(provider="cartesia", model="sonic-3", language="en"))
    cfg.turn.detector = "vad"
    return cfg


def test_stt_fallback_adapter_built_when_keyed():
    stt = build_stt(ProviderRef(provider="deepgram", model="nova-3"), S, "en-IN")
    assert isinstance(stt, lk_stt.FallbackAdapter)
    assert [provider_id(i) for i in stt._stt_instances] == ["deepgram", "sarvam"]


def test_flux_not_wrapped():
    assert isinstance(build_stt(ProviderRef(provider="deepgram", model="flux-general-multi"), S, "en-IN"), deepgram.STTv2)


def test_no_fallback_without_key():
    only_dg = Settings(_env_file=None, deepgram_api_key="d", google_api_key="g")
    assert isinstance(build_stt(ProviderRef(provider="deepgram", model="nova-3"), only_dg, "en-IN"), deepgram.STT)
    assert not isinstance(build_llm(ProviderRef(provider="google", model="gemini-2.5-flash-lite"), only_dg),
                          lk_llm.FallbackAdapter)


def test_per_agent_fallback_overrides_default():
    from livekit.plugins import silero
    # OpenAI STT is not streaming: the adapter needs a VAD to wrap it (make_session passes the prewarmed one)
    stt = build_stt(ProviderRef(provider="deepgram", model="nova-3", fallback="openai"), S, "en-IN", vad=silero.VAD.load())
    assert [provider_id(i) for i in stt._stt_instances] == ["deepgram", "openai"]
    assert isinstance(build_stt(ProviderRef(provider="deepgram", model="nova-3", fallback=""), S, "en-IN"), deepgram.STT)


def test_llm_fallback_adapter_built():
    llm = build_llm(ProviderRef(provider="google", model="gemini-2.5-flash-lite"), S)
    assert isinstance(llm, lk_llm.FallbackAdapter) and len(llm._llm_instances) == 2
    assert llm._attempt_timeout >= 10.0      # Gemini rejects request deadlines under 10 s (seen live 2026-10-23)


def test_breaker_opens_and_recovers():
    b = ProviderBreaker(open_seconds=60.0)
    assert b.is_open("k", now=0.0) is False
    b.record_failure("k", now=0.0)
    assert b.is_open("k", now=10.0) is True
    assert b.is_open("k", now=61.0) is False
    b.record_failure("k", now=100.0)
    b.record_success("k")
    assert b.is_open("k", now=101.0) is False


@pytest.mark.asyncio
async def test_breaker_substitutes_nova3_when_open():
    BREAKER.record_failure("deepgram:flux")
    try:
        session = make_session(_cfg("flux-general-multi"), S, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
        assert isinstance(session.stt, lk_stt.FallbackAdapter)
        assert session.stt._stt_instances[0].model == "nova-3"
    finally:
        BREAKER.reset()
    session = make_session(_cfg("flux-general-multi"), S, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    assert isinstance(session.stt, deepgram.STTv2)


@pytest.mark.asyncio
async def test_provider_labels_report_active_instances():
    session = make_session(_cfg("nova-3"), S, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    labels = provider_labels(session)
    assert labels["stt"] == "deepgram" and labels["llm"] == "google" and labels["tts"] == "cartesia"


def test_turn_metrics_carry_provider_fields():
    assert TurnMetrics(turn=1, stt_provider="deepgram").model_dump()["stt_provider"] == "deepgram"
    assert TurnMetrics(turn=1).model_dump()["llm_provider"] is None


def test_api_stores_provider_events(client):
    body = {"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                                             "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}
    call_id = client.post("/v1/calls", json=body, headers=H).json()["call_id"]
    client.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-1")
    ev = {"type": "provider", "seq": 1, "data": {"kind": "stt", "provider": "sarvam", "available": True, "at": 3.2}}
    assert client.post(f"/internal/calls/{call_id}/events", json=ev, headers=HI).status_code == 200
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["metrics"]["provider_events"] == [ev["data"]]
