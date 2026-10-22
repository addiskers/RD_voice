"""Synthesize a spoken test utterance (16 kHz mono PCM16 WAV) with Gemini Live, for headless STT tests.

    python scripts/make_utterance.py "Yes, I can hear you. What is this call about?" scripts/assets/user_yes.wav
"""

from __future__ import annotations

import asyncio
import os
import sys
import wave

from dotenv import load_dotenv
from google import genai
from google.genai import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.worker.audio import resample_pcm16  # noqa: E402


async def main() -> int:
    load_dotenv()
    text = sys.argv[1] if len(sys.argv) > 1 else "Yes, I can hear you. What is this call about?"
    out = sys.argv[2] if len(sys.argv) > 2 else "scripts/assets/user_yes.wav"
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
    pcm16 = resample_pcm16(bytes(pcm24), 24000, 16000)
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with wave.open(out, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(16000)
        wf.writeframes(pcm16)
    print(f"wrote {out}: {len(pcm16) / 32000:.2f}s")
    return 0 if pcm16 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
