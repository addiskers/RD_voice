"""The worker measures what each vendor will bill: STT seconds, LLM tokens per request, TTS characters."""

from types import SimpleNamespace

import httpx

from engine.config import Settings
from engine.worker.events import CallReporter


def _reporter() -> CallReporter:
    r = CallReporter(Settings(_env_file=None), httpx.AsyncClient(), "call_u")
    r.current_providers = {"stt": "deepgram", "llm": "google", "tts": "cartesia"}
    r.metrics.models = {"stt": "deepgram flux-general-multi", "llm": "google gemini-2.5-flash-lite", "tts": "cartesia sonic-3"}
    return r


def test_llm_tokens_are_summed_per_model_including_cached():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.google.llm.LLM", prompt_tokens=1200,
                                   prompt_cached_tokens=200, completion_tokens=40, cancelled=False))
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.google.llm.LLM", prompt_tokens=1300,
                                   prompt_cached_tokens=1000, completion_tokens=35, cancelled=False))
    usage = r.usage_detail()
    assert usage["llm"] == [{"provider": "google", "model": "gemini-2.5-flash-lite", "requests": 2,
                             "input_tokens": 2500, "cached_input_tokens": 1200, "output_tokens": 75, "thinking_tokens": 0}]


def test_tts_characters_count_cancelled_synthesis_too():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="tts_metrics", label="livekit.plugins.cartesia.tts.TTS", characters_count=120,
                                   audio_duration=6.1, cancelled=False))
    r.record_usage(SimpleNamespace(type="tts_metrics", label="livekit.plugins.cartesia.tts.TTS", characters_count=80,
                                   audio_duration=0.0, cancelled=True))      # preemptive synthesis thrown away: still billed
    usage = r.usage_detail()
    assert usage["tts"] == [{"provider": "cartesia", "model": "sonic-3", "requests": 2, "characters": 200,
                             "cancelled_characters": 80, "audio_seconds": 6.1}]


def test_failover_splits_llm_usage_by_vendor():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.google.llm.LLM", prompt_tokens=100,
                                   prompt_cached_tokens=0, completion_tokens=10, cancelled=False))
    r.current_providers["llm"] = "groq"
    r.metrics.models["llm"] = "google gemini-2.5-flash-lite"          # the configured primary stays the same
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.openai.llm.LLM", prompt_tokens=100,
                                   prompt_cached_tokens=0, completion_tokens=10, cancelled=False, model="openai/gpt-oss-20b"))
    usage = r.usage_detail()
    assert [(u["provider"], u["model"]) for u in usage["llm"]] == [("google", "gemini-2.5-flash-lite"), ("groq", "openai/gpt-oss-20b")]


def test_stt_seconds_is_the_streamed_duration():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="stt_metrics", label="livekit.plugins.deepgram.stt.STTv2", audio_duration=3.2))
    usage = r.usage_detail(call_seconds=61.0)
    assert usage["stt"] == [{"provider": "deepgram", "model": "flux-general-multi", "seconds": 61.0,
                             "reported_seconds": 3.2}]


def test_realtime_tokens_by_modality():
    r = _reporter()
    r.metrics.models = {"llm": "google gemini-2.5-flash-native-audio-preview-12-2025"}
    r.current_providers = {"llm": "google"}
    r.record_usage(SimpleNamespace(type="realtime_model_metrics", label="livekit.plugins.google.realtime", cancelled=False,
                                   input_tokens=3000, output_tokens=800,
                                   input_token_details=SimpleNamespace(audio_tokens=2500, text_tokens=500, cached_tokens=0),
                                   output_token_details=SimpleNamespace(audio_tokens=700, text_tokens=100)))
    usage = r.usage_detail()
    assert usage["realtime"] == [{"provider": "google", "model": "gemini-2.5-flash-native-audio-preview-12-2025",
                                  "requests": 1, "audio_input_tokens": 2500, "text_input_tokens": 500,
                                  "cached_input_tokens": 0, "audio_output_tokens": 700, "text_output_tokens": 100}]


def test_gemini_thinking_tokens_recovered_from_total():
    # Google's plugin reports candidates tokens as completion_tokens; thinking tokens are billed as output
    # but only visible as total - prompt - completion.
    r = _reporter()
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.google.llm.LLM", prompt_tokens=1000,
                                   prompt_cached_tokens=0, completion_tokens=50, total_tokens=1300, cancelled=False))
    line = r.usage_detail()["llm"][0]
    assert line["output_tokens"] == 300 and line["thinking_tokens"] == 250


def test_reasoning_tokens_already_inside_completion_are_not_double_counted():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.openai.llm.LLM", prompt_tokens=100,
                                   prompt_cached_tokens=0, completion_tokens=80, reasoning_tokens=30, total_tokens=180,
                                   cancelled=False))
    line = r.usage_detail()["llm"][0]
    assert line["output_tokens"] == 80 and line["thinking_tokens"] == 30


def test_usage_follows_the_vendor_that_served_the_request_not_the_configured_one():
    # LiveKit stamps every metric with the instance that served it (FallbackAdapter included); after a
    # TTS failover the characters belong to the fallback vendor even though the agent is configured for cartesia.
    r = _reporter()
    meta = SimpleNamespace(model_provider="Sarvam", model_name="bulbul:v3")
    r.record_usage(SimpleNamespace(type="tts_metrics", label="livekit.plugins.sarvam.tts.TTS", characters_count=40,
                                   audio_duration=2.0, cancelled=False, metadata=meta))
    r.record_usage(SimpleNamespace(type="tts_metrics", label="livekit.plugins.cartesia.tts.TTS", characters_count=10,
                                   audio_duration=0.5, cancelled=False,
                                   metadata=SimpleNamespace(model_provider="Cartesia", model_name="sonic-3")))
    tts = {(u["provider"], u["model"]): u["characters"] for u in r.usage_detail()["tts"]}
    assert tts == {("sarvam", "bulbul:v3"): 40, ("cartesia", "sonic-3"): 10}


def test_groq_served_through_the_openai_plugin_is_billed_as_groq():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="llm_metrics", label="livekit.plugins.openai.llm.LLM", prompt_tokens=100,
                                   prompt_cached_tokens=0, completion_tokens=10, total_tokens=110, cancelled=False,
                                   metadata=SimpleNamespace(model_provider="api.groq.com", model_name="openai/gpt-oss-20b")))
    line = r.usage_detail()["llm"][0]
    assert (line["provider"], line["model"]) == ("groq", "openai/gpt-oss-20b")


def test_stt_failover_splits_the_call_seconds_instead_of_billing_both_vendors_for_the_whole_call():
    r = _reporter()
    r.record_usage(SimpleNamespace(type="stt_metrics", label="livekit.plugins.deepgram.stt.STT", audio_duration=35.0,
                                   metadata=SimpleNamespace(model_provider="Deepgram", model_name="flux-general-multi")))
    r.record_usage(SimpleNamespace(type="stt_metrics", label="livekit.plugins.sarvam.stt.STT", audio_duration=20.0,
                                   metadata=SimpleNamespace(model_provider="Sarvam", model_name="saaras:v4")))
    lines = {(u["provider"], u["model"]): u["seconds"] for u in r.usage_detail(call_seconds=60.0)["stt"]}
    assert lines == {("deepgram", "flux-general-multi"): 40.0, ("sarvam", "saaras:v4"): 20.0}
    assert sum(lines.values()) == 60.0


def test_ended_bills_the_duration_it_is_given_not_the_time_including_post_call_work():
    r = _reporter()
    posted = []
    r.post = lambda etype, data: posted.append((etype, data))
    r.ended("workflow_end", duration_s=12.5)
    etype, data = posted[-1]
    assert etype == "ended" and data["duration_s"] == 12.5
    assert data["usage_detail"]["stt"][0]["seconds"] == 12.5
