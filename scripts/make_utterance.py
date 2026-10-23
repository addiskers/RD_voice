"""Synthesize a spoken test utterance (16 kHz mono PCM16 WAV) with Gemini Live, for headless STT tests,
or degrade an existing WAV to what the phone network delivers (300-3400 Hz, 8 kHz, G.711).

    python scripts/make_utterance.py "Yes, I can hear you. What is this call about?" scripts/assets/user_yes.wav
    python scripts/make_utterance.py "text" scripts/assets/tel_user_x.wav --telephony     # synthesize, then degrade
    python scripts/make_utterance.py --telephony scripts/assets/user_yes.wav scripts/assets/tel_user_yes.wav
"""

from __future__ import annotations

import asyncio
import os
import sys
import wave

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.worker.audio import resample_pcm16, telephony_degrade  # noqa: E402


def _write(path: str, pcm: bytes, rate: int) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(rate)
        wf.writeframes(pcm)


def degrade_file(src: str, dst: str) -> float:
    with wave.open(src, "rb") as wf:
        assert wf.getnchannels() == 1 and wf.getsampwidth() == 2, "need mono PCM16"
        rate, pcm = wf.getframerate(), wf.readframes(wf.getnframes())
    pcm8 = telephony_degrade(pcm, rate)
    _write(dst, pcm8, 8000)
    return len(pcm8) / 16000


async def synthesize_sarvam(text: str, *, speaker: str = "rahul", language: str = "en-IN",
                            model: str = "bulbul:v3") -> bytes:
    """Indian-accented test audio (English or an Indic language) from Sarvam bulbul; returns 16 kHz PCM16."""
    import base64
    import io

    import httpx
    from dotenv import load_dotenv
    load_dotenv()
    key = os.environ.get("SARVAM_API_KEY", "")
    assert key, "SARVAM_API_KEY is not set"
    payload = {"text": text, "target_language_code": language, "speaker": speaker, "pace": 1.0,
               "speech_sample_rate": 16000, "model": model}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post("https://api.sarvam.ai/text-to-speech", json=payload,
                              headers={"api-subscription-key": key, "Content-Type": "application/json"})
    if r.status_code != 200:
        raise RuntimeError(f"Sarvam TTS {r.status_code}: {r.text[:300]}")
    audios = r.json().get("audios") or []
    assert audios, "Sarvam TTS returned no audio"
    with wave.open(io.BytesIO(base64.b64decode(audios[0])), "rb") as wf:
        rate, pcm = wf.getframerate(), wf.readframes(wf.getnframes())
        assert wf.getnchannels() == 1 and wf.getsampwidth() == 2
    return pcm if rate == 16000 else resample_pcm16(pcm, rate, 16000)


async def synthesize(text: str) -> bytes:
    from dotenv import load_dotenv
    from google import genai
    from google.genai import types
    load_dotenv()
    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])
    config = types.LiveConnectConfig(
        response_modalities=[types.Modality.AUDIO],
        speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(
            prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name="Charon"))),
        system_instruction="You are a voice actor. Say exactly the sentence the user gives you, word for word, "
                           "naturally, and nothing else.",
    )
    pcm24 = bytearray()
    async with client.aio.live.connect(model=os.environ.get("DEFAULT_REALTIME_MODEL", "gemini-3.1-flash-live-preview"),
                                       config=config) as session:
        await session.send_client_content(
            turns=[types.Content(role="user", parts=[types.Part(text=f'Say exactly: "{text}"')])], turn_complete=True)
        async for msg in session.receive():
            sc = msg.server_content
            if sc and sc.model_turn and sc.model_turn.parts:
                for part in sc.model_turn.parts:
                    if part.inline_data and part.inline_data.data:
                        pcm24 += part.inline_data.data
            if sc and sc.turn_complete:
                break
    return resample_pcm16(bytes(pcm24), 24000, 16000)


async def main() -> int:
    """Flags: --telephony (degrade to 8 kHz phone audio), --voice sarvam[:speaker] (Indian-accented voice,
    default Gemini), --lang en-IN|hi-IN|gu-IN (Sarvam target language, default en-IN)."""
    args = sys.argv[1:]
    voice, lang = "gemini", "en-IN"
    argv: list[str] = []
    i = 0
    while i < len(args):
        if args[i] == "--voice":
            voice, i = args[i + 1], i + 2
        elif args[i] == "--lang":
            lang, i = args[i + 1], i + 2
        elif args[i] == "--telephony":
            i += 1
        else:
            argv.append(args[i])
            i += 1
    telephony = "--telephony" in args
    if telephony and len(argv) == 2 and os.path.exists(argv[0]):
        secs = degrade_file(argv[0], argv[1])
        print(f"wrote {argv[1]}: {secs:.2f}s at 8 kHz (band-limited, G.711)")
        return 0
    text = argv[0] if argv else "Yes, I can hear you. What is this call about?"
    out = argv[1] if len(argv) > 1 else "scripts/assets/user_yes.wav"
    if voice.startswith("sarvam"):
        speaker = voice.split(":", 1)[1] if ":" in voice else "rahul"
        pcm16 = await synthesize_sarvam(text, speaker=speaker, language=lang)
    else:
        pcm16 = await synthesize(text)
    if telephony:
        pcm = telephony_degrade(pcm16, 16000)
        _write(out, pcm, 8000)
        print(f"wrote {out}: {len(pcm) / 16000:.2f}s at 8 kHz (band-limited, G.711)")
    else:
        _write(out, pcm16, 16000)
        print(f"wrote {out}: {len(pcm16) / 32000:.2f}s")
    return 0 if pcm16 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
