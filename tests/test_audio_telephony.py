"""Audio on the phone path: soxr-backed resampler, telephony band-limiting, 8 kHz STT input."""

import math
import struct

import numpy as np
import pytest

from engine.config import Settings
from engine.schemas.agent import AgentConfig, Mode, ProviderRef, VoiceRef
from engine.worker import audio
from engine.worker.audio import StreamResampler, telephony_degrade
from engine.worker.pipeline import make_session, unwrap_stt
from engine.worker.tools import CallState


def _sine(rate: int, seconds: float, freq: float, amp: int = 8000) -> bytes:
    n = int(rate * seconds)
    return struct.pack(f"<{n}h", *(int(amp * math.sin(2 * math.pi * freq * i / rate)) for i in range(n)))


def _snr_db(ref: bytes, out: bytes, max_lag: int = 200) -> float:
    """SNR of ``out`` against ``ref`` after aligning for the resampler's group delay."""
    a = np.frombuffer(ref, dtype=np.int16).astype(np.float64)
    b = np.frombuffer(out, dtype=np.int16).astype(np.float64)
    n = min(len(a), len(b)) - max_lag
    best = (-1.0, 0)
    for lag in range(-max_lag, max_lag + 1):
        seg_a = a[max_lag: max_lag + n - max_lag]
        seg_b = b[max_lag + lag: max_lag + lag + n - max_lag]
        c = float(np.dot(seg_a, seg_b))
        if c > best[0]:
            best = (c, lag)
    lag = best[1]
    seg_a = a[max_lag + 100: max_lag + n - max_lag - 100]
    seg_b = b[max_lag + lag + 100: max_lag + lag + n - max_lag - 100]
    noise = seg_a - seg_b
    return 10 * math.log10(float(np.sum(seg_a ** 2)) / max(1.0, float(np.sum(noise ** 2))))


def test_stream_resampler_roundtrip_snr():
    tone = _sine(8000, 1.0, 1000.0)
    up, down = StreamResampler(8000, 16000), StreamResampler(16000, 8000)
    mid = up.process(tone) + up.flush()
    back = down.process(mid) + down.flush()
    assert abs(len(back) - len(tone)) <= 64
    assert _snr_db(tone, back) >= 35.0


def test_stream_resampler_is_stateful_across_frames():
    tone = _sine(16000, 1.0, 440.0)
    whole_rs = StreamResampler(16000, 8000)
    whole = whole_rs.process(tone) + whole_rs.flush()
    chunked_rs = StreamResampler(16000, 8000)
    chunks = b"".join(chunked_rs.process(tone[i:i + 640]) for i in range(0, len(tone), 640)) + chunked_rs.flush()
    a = np.frombuffer(whole, dtype=np.int16).astype(np.int32)
    b = np.frombuffer(chunks, dtype=np.int16).astype(np.int32)
    n = min(len(a), len(b))
    assert abs(len(a) - len(b)) <= 16 and n > 7000
    assert int(np.max(np.abs(a[:n] - b[:n]))) <= 2


def test_identity_resampler_is_passthrough():
    pcm = _sine(8000, 0.02, 440.0)
    rs = StreamResampler(8000, 8000)
    assert rs.process(pcm) == pcm and rs.flush() == b""
    assert audio.resample_pcm16(pcm, 8000, 8000) == pcm


def test_telephony_degrade_bandlimits():
    rate, n = 16000, 16000
    t = np.arange(n) / rate
    sig = (6000 * np.sin(2 * np.pi * 1000 * t) + 6000 * np.sin(2 * np.pi * 3800 * t)).astype(np.int16).tobytes()
    out = telephony_degrade(sig, rate)
    samples = np.frombuffer(out, dtype=np.int16).astype(np.float64)
    assert abs(len(samples) - 8000) <= 64
    spectrum = np.abs(np.fft.rfft(samples[500:-500] * np.hanning(len(samples) - 1000)))
    freqs = np.fft.rfftfreq(len(samples) - 1000, d=1 / 8000)
    p1k = spectrum[np.argmin(np.abs(freqs - 1000))]
    p38 = spectrum[np.argmin(np.abs(freqs - 3800))]
    assert 20 * math.log10(p1k / max(p38, 1e-9)) >= 20.0


@pytest.mark.asyncio
async def test_make_session_passes_input_sample_rate():
    settings = Settings(_env_file=None, deepgram_api_key="d", google_api_key="g", cartesia_api_key="c")
    cfg = AgentConfig(name="a", mode=Mode.cascaded, instructions="x",
                      stt=ProviderRef(provider="deepgram", model="nova-3"),
                      llm=ProviderRef(provider="google", model="gemini-2.5-flash-lite"),
                      tts=VoiceRef(provider="cartesia", model="sonic-3", language="en"))
    cfg.turn.detector = "vad"
    session = make_session(cfg, settings, prewarmed={"vad": None}, userdata=CallState(call_id="c"), input_sample_rate=8000)
    assert unwrap_stt(session.stt)._opts.sample_rate == 8000
    session = make_session(cfg, settings, prewarmed={"vad": None}, userdata=CallState(call_id="c"))
    assert unwrap_stt(session.stt)._opts.sample_rate == 16000
