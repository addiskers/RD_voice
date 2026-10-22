"""Worker pieces that can be exercised without a live session or provider keys."""

import json
from pathlib import Path

import httpx

from engine.config import Settings
from engine.schemas.agent import AgentConfig, Mode, ToolDef, ToolType
from engine.worker.graph.runtime import GraphRuntime, NodeAgent
from engine.worker.graph.validate import compile_workflow
from engine.worker.pipeline import build_turn_handling, make_session
from engine.worker.tools import CallState, build_tools

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def _cfg() -> AgentConfig:
    agent = json.loads((EXAMPLES / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
    agent["workflow"] = json.loads((EXAMPLES / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))
    return AgentConfig.model_validate(agent)


def test_tools_built_from_defs():
    cfg = _cfg()
    tools = build_tools(cfg.tools, httpx.AsyncClient())
    assert set(tools) == {"record_outcome", "end_call"}
    info = tools["record_outcome"].info
    assert info.name == "record_outcome" and "status" in info.raw_schema["parameters"]["properties"]
    http_tool = ToolDef(id="slots", type=ToolType.http, name="check_slots", description="d", url="https://p/slots",
                        parameters={"type": "object", "properties": {"date": {"type": "string"}}})
    assert "slots" in build_tools([http_tool], httpx.AsyncClient())


def test_graph_runtime_instructions_and_transition_tools():
    cfg = _cfg()
    graph = compile_workflow(cfg.effective_workflow())
    state = CallState(call_id="call_t", variables={"first_name": "Pratik", "event_name": "Workshop",
                                                   "event_date": "the eleventh", "event_time": "7 pm",
                                                   "event_venue": "DoubleTree"})
    rt = GraphRuntime(graph, cfg, state, build_tools(cfg.tools, httpx.AsyncClient()))
    start = graph.node(graph.start_id)
    text = rt.instructions_for(start)
    assert "EO Gujarat" in text and "Pratik" in text and "confirmed, wrong_number" in text
    assert rt.render(start.data.greeting) == "Hello! Am I speaking to Pratik?"
    rsvp_tools = rt.tools_for(graph.node("rsvp"))
    names = sorted(t.info.name for t in rsvp_tools)
    assert names == ["said_no_or_callback", "said_yes"]
    kids_tools = rt.tools_for(graph.node("kids"))
    assert {t.info.name for t in kids_tools} == {"kids_answered"}
    agent = NodeAgent(rt, "closing_yes")
    assert agent.id == "node:closing_yes" and "closing step" in agent.instructions
    # closing nodes declare their outcome on the node: no record_outcome / end_call round-trips
    assert {t.info.name for t in agent.tools} == set()
    assert graph.node("closing_yes").data.outcome == {"status": "yes"}


def test_flat_mode_finish_tools_and_prompt():
    cfg = _cfg()
    graph = compile_workflow(cfg.effective_workflow())
    state = CallState(call_id="call_t", variables={"first_name": "Pratik"})
    rt = GraphRuntime(graph, cfg, state, build_tools(cfg.tools, httpx.AsyncClient()))
    names = {t.info.name for t in rt.all_node_tools()}
    assert {"finish_closing_yes", "finish_closing_no", "finish_wrong_number", "end_call"} <= names
    text = rt.flatten_instructions()
    assert "finish_closing_no" in text and "STEP 1" in text
    assert "CALL RULE" in text and "FIRST call the finish_closing_no tool" in text
    assert text.rstrip().endswith(GraphRuntime.LANGUAGE_POLICY)   # policy repeated last for S2S recency


def test_language_hint_from_script():
    from engine.worker.graph.runtime import language_hint
    assert "Hinglish" in language_hint("हां, मैं प्रतीक बोल रहा हूं")
    assert "Gujarati" in language_hint("હા, હું પ્રતીક બોલું છું")
    assert language_hint("Yes, this is Pratik speaking") == ""


async def test_user_turn_adds_language_hint():
    from livekit.agents import llm as lk_llm
    cfg = _cfg()
    graph = compile_workflow(cfg.effective_workflow())
    state = CallState(call_id="call_t", variables={"first_name": "Pratik"})
    rt = GraphRuntime(graph, cfg, state, build_tools(cfg.tools, httpx.AsyncClient()))
    agent = NodeAgent(rt, "invite")
    ctx = lk_llm.ChatContext.empty()
    msg = lk_llm.ChatMessage(role="user", content=["नहीं, इस बार नहीं आ पाऊँगा"])
    await agent.on_user_turn_completed(ctx, msg)
    assert any(i.role == "system" and "Hinglish" in (i.text_content or "") for i in ctx.items)
    ctx2 = lk_llm.ChatContext.empty()
    await agent.on_user_turn_completed(ctx2, lk_llm.ChatMessage(role="user", content=["yes"]))
    assert not ctx2.items


def test_turn_handling_mapping():
    cfg = _cfg()
    th = build_turn_handling(cfg.turn, Mode.cascaded, turn_detector="DETECTOR")
    assert th["turn_detection"] == "DETECTOR"
    assert th["endpointing"] == {"min_delay": 0.25, "max_delay": 2.0}
    assert th["interruption"]["resume_false_interruption"] is True and "mode" not in th["interruption"]
    assert th["preemptive_generation"] == {"enabled": True, "preemptive_tts": True}
    assert build_turn_handling(cfg.turn, Mode.s2s)["turn_detection"] == "realtime_llm"
    assert build_turn_handling(cfg.turn, Mode.cascaded, turn_detector="DETECTOR", stt_turns=True)["turn_detection"] == "stt"
    cfg.turn.detector = "vad"
    cfg.turn.interruption_mode = "vad"
    th = build_turn_handling(cfg.turn, Mode.cascaded)
    assert th["turn_detection"] == "vad" and th["interruption"]["mode"] == "vad"


async def test_reporter_listeners_receive_posted_events():
    from engine.worker.events import CallReporter
    r = CallReporter(Settings(_env_file=None), httpx.AsyncClient(), "call_x")
    got = []
    r.listeners.append(lambda t, d: got.append((t, d)))
    r.listeners.append(lambda t, d: 1 / 0)          # a failing observer must not break posting
    r.post("metrics", r.metrics.recompute().model_dump(mode="json"))
    assert got and got[0][0] == "metrics" and "turns" in got[0][1] and "total_p50" in got[0][1]
    assert r.seq == 1


def test_schema_defaults_for_latency():
    from engine.schemas.agent import TurnConfig
    t = TurnConfig()
    assert t.min_endpointing_delay == 0.25 and t.preemptive_tts is True


def test_llm_and_stt_latency_options():
    from engine.schemas.agent import ProviderRef
    from engine.worker.pipeline import build_llm, build_stt, stt_provides_turns, unwrap_llm
    settings = Settings(_env_file=None, deepgram_api_key="dg", google_api_key="gk", groq_api_key="gq")
    # build_llm wraps the primary in a FallbackAdapter when groq is keyed; inspect the primary
    llm = unwrap_llm(build_llm(ProviderRef(provider="google", model="gemini-2.5-flash", options={"temperature": 0.4}), settings))
    assert llm._opts.thinking_config == {"thinking_budget": 0}          # thinking off by default for 2.5
    llm = unwrap_llm(build_llm(ProviderRef(provider="google", model="gemini-2.5-flash", options={"thinking_budget": -1}), settings))
    assert llm._opts.thinking_config is None or llm._opts.thinking_config == {} or not llm._opts.thinking_config
    groq = unwrap_llm(build_llm(ProviderRef(provider="groq", model="llama-3.1-8b-instant"), settings))
    assert "groq.com" in str(groq._client.base_url)
    flux = build_stt(ProviderRef(provider="deepgram", model="flux-general-multi"), settings, "en-IN")
    assert stt_provides_turns(flux) and flux._opts.language_hint == ["en", "hi"]
    assert flux._opts.eot_threshold == 0.6 and flux._opts.eager_eot_threshold == 0.4 and flux._opts.eot_timeout_ms == 1800
    from engine.schemas.agent import TurnConfig
    assert TurnConfig().max_endpointing_delay == 2.0
    nova = build_stt(ProviderRef(provider="deepgram", model="nova-3"), settings, "en-IN")
    assert not stt_provides_turns(nova)


async def test_make_session_constructs_providers_without_network():
    # AgentSession binds to the running event loop at construction, hence an async test
    settings = Settings(_env_file=None, deepgram_api_key="dg", google_api_key="gk", cartesia_api_key="ck")
    cfg = _cfg()
    cfg.turn.detector = "vad"          # avoid loading the turn-detector model in tests
    session = make_session(cfg, settings, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    assert session is not None and session.userdata.call_id == "c"
    cfg.mode = Mode.s2s
    session = make_session(cfg, settings, prewarmed={"vad": None}, userdata=CallState(call_id="c2"))
    assert session is not None
