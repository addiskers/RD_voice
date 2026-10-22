import base64
import json
import math
import struct

from engine.worker import audio, stream_protocol as sp
from engine.worker.guards import GuardPolicy, SentenceGate, guard_text, split_sentences


def _sine_pcm(rate: int, seconds: float, freq: float = 440.0, amp: int = 8000) -> bytes:
    n = int(rate * seconds)
    return struct.pack(f"<{n}h", *(int(amp * math.sin(2 * math.pi * freq * i / rate)) for i in range(n)))


def test_mulaw_roundtrip_is_close():
    pcm8k = _sine_pcm(8000, 0.02)
    ulaw = audio.pcm16_to_mulaw(pcm8k, 8000)
    assert len(ulaw) == audio.FRAME_BYTES
    back = audio.mulaw_to_pcm16(ulaw)
    a = struct.unpack("<160h", pcm8k)
    b = struct.unpack("<160h", back)
    err = sum(abs(x - y) for x, y in zip(a, b)) / len(a)
    assert err < 200  # mu-law quantisation on an 8000-amplitude sine


def test_upsample_and_downsample_sizes():
    ulaw = bytes([0xFF]) * audio.FRAME_BYTES
    pcm16k = audio.mulaw_to_pcm16k(ulaw)
    assert len(pcm16k) == 320 * 2
    assert audio.pcm16_energy(pcm16k) == 0.0
    pcm24k = _sine_pcm(24000, 0.02)
    assert len(audio.pcm16_to_mulaw(pcm24k, 24000)) == audio.FRAME_BYTES
    pcm48k = _sine_pcm(48000, 0.02)
    assert len(audio.pcm16_to_mulaw(pcm48k, 48000)) == audio.FRAME_BYTES
    assert len(audio.pcm16_to_mulaw(pcm16k, 16000)) == audio.FRAME_BYTES


def test_energy_gate_distinguishes_speech_from_silence():
    loud = audio.pcm16_to_mulaw(_sine_pcm(8000, 0.02, amp=6000), 8000)
    assert audio.mulaw_frame_meansquare(audio.SILENCE_FRAME_ULAW) < 10
    assert audio.mulaw_frame_meansquare(loud) > 500 ** 2


def test_resample_pcm16():
    pcm = _sine_pcm(16000, 0.1)
    out = audio.resample_pcm16(pcm, 16000, 24000)
    assert abs(len(out) // 2 - 2400) <= 1
    assert audio.resample_pcm16(pcm, 16000, 16000) == pcm


def test_stream_protocol_parse_and_build():
    start = {"event": "start", "start": {"streamId": "st1", "callId": "c1", "accountId": "MA",
                                           "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000}},
             "extra_headers": "X-Call-Id=call_123,X-Caller=%2B91987"}
    s = sp.parse_start(start)
    assert (s.stream_id, s.call_id, s.sample_rate) == ("st1", "c1", 8000)
    assert s.extra_headers == {"X-Call-Id": "call_123", "X-Caller": "+91987"}
    media = {"event": "media", "media": {"payload": base64.b64encode(b"\xff" * 160).decode(), "track": "inbound"}}
    assert sp.event_type(media) == "media" and len(base64.b64decode(sp.media_payload(media))) == 160
    assert sp.dtmf_digit({"event": "dtmf", "dtmf": {"digit": "5"}}) == "5"
    play = json.loads(sp.play_audio("AAAA", "st1"))
    assert play["event"] == "playAudio" and play["media"]["sampleRate"] == 8000 and play["streamId"] == "st1"
    assert json.loads(sp.clear_audio("st1")) == {"event": "clearAudio", "streamId": "st1"}
    assert json.loads(sp.checkpoint("greeting", "st1"))["name"] == "greeting"


def test_guard_single_closing_and_repeats():
    text = ("So glad you can make it. See you on the eleventh! Ah, so you're travelling, no problem. "
            "See you on the eleventh, take care!")
    out = guard_text(text, GuardPolicy(max_sentences=6))
    assert out.count("See you") == 1
    text2 = "Registration is mandatory. Registration is mandatory. Please register soon."
    assert guard_text(text2) == "Registration is mandatory. Please register soon."


def test_guard_sentence_cap_and_disabled():
    text = "One. Two. Three. Four. Five. Six."
    assert guard_text(text, GuardPolicy(max_sentences=2)) == "One. Two."
    assert guard_text(text, GuardPolicy(enabled=False, max_sentences=1)) == text


def test_sentence_gate_streaming():
    gate = SentenceGate(GuardPolicy(max_sentences=3))
    out = []
    for delta in ["Hello there", ". I am ", "calling from EO. I am calling ", "from EO. Can you come?"]:
        out += list(gate.feed(delta))
    out += list(gate.flush())
    assert out == ["Hello there.", "I am calling from EO.", "Can you come?"]
    assert gate.dropped == ["I am calling from EO."]


def test_split_sentences_handles_devanagari_danda():
    assert split_sentences("नमस्ते। आप कैसे हैं? Good.") == ["नमस्ते।", "आप कैसे हैं?", "Good."]
