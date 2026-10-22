"""Standalone Gemini Live check (no LiveKit): connect, send a 'speak first' text turn, count audio bytes.

    python scripts/gemini_live_check.py [--model gemini-3.1-flash-live-preview] [--key ...]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time

from dotenv import load_dotenv
from google import genai
from google.genai import types


async def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=os.environ.get("DEFAULT_REALTIME_MODEL", "gemini-3.1-flash-live-preview"))
    ap.add_argument("--key", default=os.environ.get("GOOGLE_API_KEY", ""))
    ap.add_argument("--voice", default="Aoede")
    args = ap.parse_args()

    client = genai.Client(api_key=args.key)
    config = types.LiveConnectConfig(
        response_modalities=[types.Modality.AUDIO],
        speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(
            prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=args.voice))),
        system_instruction="You are a test agent. Reply in one short sentence.",
        output_audio_transcription=types.AudioTranscriptionConfig(),
    )
    t0 = time.monotonic()
    audio = 0
    text = ""
    try:
        async with client.aio.live.connect(model=args.model, config=config) as session:
            print(f"connected in {time.monotonic() - t0:.2f}s")
            await session.send_client_content(
                turns=[types.Content(role="user", parts=[types.Part(text="[Call connected. Greet the caller now.]")])],
                turn_complete=True)
            first = None
            async for msg in session.receive():
                sc = msg.server_content
                if sc is None:
                    continue
                if sc.model_turn and sc.model_turn.parts:
                    for part in sc.model_turn.parts:
                        if part.inline_data and part.inline_data.data:
                            audio += len(part.inline_data.data)
                            if first is None:
                                first = time.monotonic() - t0
                if sc.output_transcription and sc.output_transcription.text:
                    text += sc.output_transcription.text
                if sc.turn_complete:
                    break
        print(f"audio bytes={audio} (~{audio / 48000:.2f}s @24k), first audio after {first and round(first, 2)}s")
        print("transcript:", text.strip())
        return 0 if audio > 0 else 1
    except Exception as e:
        print("ERROR:", type(e).__name__, str(e)[:600])
        return 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
