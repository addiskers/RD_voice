"""The TTS fallback vendor receives a language code it accepts, whatever the primary was configured with."""

import pytest

from engine.config import Settings
from engine.schemas.agent import VoiceRef
from engine.worker.pipeline import _build_tts_single, sarvam_language


@pytest.mark.parametrize("given,expected", [("en", "en-IN"), ("hi", "hi-IN"), ("gu", "gu-IN"), ("en-IN", "en-IN"),
                                            ("hi-IN", "hi-IN"), ("", "en-IN"), (None, "en-IN"), ("fr", "en-IN")])
def test_sarvam_language_codes_are_regionalised(given, expected):
    assert sarvam_language(given, default="en-IN") == expected


def test_sarvam_tts_built_as_fallback_for_an_english_cartesia_agent_gets_en_in():
    settings = Settings(_env_file=None, sarvam_api_key="test-key")
    tts = _build_tts_single(VoiceRef(provider="sarvam", language="en"), settings, "en-IN")
    assert tts._opts.target_language_code == "en-IN"


def test_tts_fallback_is_never_the_primary_vendor_itself():
    from engine.worker.pipeline import tts_fallback_vendor
    s = Settings(_env_file=None, cartesia_api_key="c", sarvam_api_key="s", tts_fallback_provider="cartesia")
    assert tts_fallback_vendor("cartesia", s) == "sarvam"           # the shipped default was cartesia -> cartesia: no fallback
    assert tts_fallback_vendor("sarvam", s) == "cartesia"
    assert tts_fallback_vendor("cartesia", Settings(_env_file=None, cartesia_api_key="c", tts_fallback_provider="cartesia")) == ""
    assert tts_fallback_vendor("cartesia", Settings(_env_file=None, cartesia_api_key="c", sarvam_api_key="s",
                                                    tts_fallback_provider="")) == ""   # explicit off stays off


def test_tts_fallback_switches_vendor_on_the_first_failure():
    from engine.worker.pipeline import build_tts
    s = Settings(_env_file=None, cartesia_api_key="c", sarvam_api_key="s", tts_fallback_provider="cartesia")
    adapter = build_tts(VoiceRef(provider="cartesia", model="sonic-3", language="en"), s, "en-IN")
    assert isinstance(adapter, __import__("livekit.agents.tts", fromlist=["FallbackAdapter"]).FallbackAdapter)
    assert adapter._max_retry_per_tts == 0
    assert [type(t).__module__.split(".")[2] for t in adapter._tts_instances] == ["cartesia", "sarvam"]
    assert adapter._tts_instances[1]._opts.target_language_code == "en-IN"


@pytest.mark.asyncio
async def test_tts_fallback_gives_the_primary_six_seconds_not_ten():
    # measured 2026-10-23: with the primary returning 402 on connect, LiveKit's adapter still waited its 10 s
    # default timeout before switching, so every call opened with 10 s of silence. A voice that has produced
    # nothing in 6 s is dead for a phone call (3 s dropped Sarvam mid-sentence: the deadline doubles as its receive timeout).
    from engine.worker.pipeline import TTS_ATTEMPT_TIMEOUT_S, build_tts
    s = Settings(_env_file=None, cartesia_api_key="c", sarvam_api_key="s")
    adapter = build_tts(VoiceRef(provider="cartesia", model="sonic-3", language="en"), s, "en-IN")
    stream = adapter.stream()
    chunked = adapter.synthesize("hello")
    try:
        assert stream._conn_options.timeout == TTS_ATTEMPT_TIMEOUT_S == 6.0
        assert chunked._conn_options.timeout == 6.0
        assert adapter._max_retry_per_tts == 0
    finally:
        await stream.aclose()
        await chunked.aclose()
