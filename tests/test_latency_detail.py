"""Per-turn latency detail: model names on the call record, node and transition markers per turn."""

import httpx
import pytest

from engine.config import Settings
from engine.schemas.agent import AgentConfig, Mode, ProviderRef, VoiceRef
from engine.schemas.calls import CallMetrics, TurnMetrics
from engine.worker.events import CallReporter
from engine.worker.pipeline import make_session, model_labels
from engine.worker.tools import CallState

S = Settings(_env_file=None, deepgram_api_key="d", google_api_key="g", cartesia_api_key="c")


def _cfg() -> AgentConfig:
    cfg = AgentConfig(name="a", mode=Mode.cascaded, instructions="x",
                      stt=ProviderRef(provider="deepgram", model="nova-3"),
                      llm=ProviderRef(provider="google", model="gemini-2.5-flash-lite"),
                      tts=VoiceRef(provider="cartesia", model="sonic-3", language="en"))
    cfg.turn.detector = "vad"
    return cfg


@pytest.mark.asyncio
async def test_model_labels_report_provider_and_model():
    session = make_session(_cfg(), S, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    labels = model_labels(session)
    assert labels == {"stt": "deepgram nova-3", "llm": "google gemini-2.5-flash-lite", "tts": "cartesia sonic-3"}


@pytest.mark.asyncio
async def test_reporter_stamps_models_node_and_transition():
    session = make_session(_cfg(), S, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    r = CallReporter(S, httpx.AsyncClient(), "call_x")
    r.attach(session)
    assert r.metrics.models == {"stt": "deepgram nova-3", "llm": "google gemini-2.5-flash-lite", "tts": "cartesia sonic-3"}
    r.node_id = "identity"
    first = r._new_turn(turn=1, at=1.0)
    r.metrics.turns.append(first)
    r.node_id = "invite"
    second = r._new_turn(turn=2, at=5.0)
    r.metrics.turns.append(second)
    third = r._new_turn(turn=3, at=9.0)
    assert (first.node, first.transition) == ("identity", False)
    assert (second.node, second.transition) == ("invite", True)      # the node changed since the previous turn
    assert (third.node, third.transition) == ("invite", False)


def test_schema_defaults_for_detail_fields():
    assert TurnMetrics(turn=1).model_dump()["transition"] is False
    assert CallMetrics().model_dump()["models"] == {}
