"""Standalone LiveKit + Gemini Live check without any transport: silence in, counting sink out.

    python scripts/livekit_rt_check.py [--model ...] [--seconds 12]
Prints every AgentSession event and how much agent audio the sink received.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time

from dotenv import load_dotenv
from livekit import rtc
from livekit.agents import Agent, AgentSession
from livekit.agents.voice.io import AudioInput, AudioOutput, AudioOutputCapabilities


class SilenceInput(AudioInput):
    def __init__(self):
        super().__init__(label="silence")
        self._closed = False

    async def __anext__(self) -> rtc.AudioFrame:
        if self._closed:
            raise StopAsyncIteration
        await asyncio.sleep(0.02)
        return rtc.AudioFrame(data=bytes(640), sample_rate=16000, num_channels=1, samples_per_channel=320)


class CountingOutput(AudioOutput):
    def __init__(self):
        super().__init__(label="count", capabilities=AudioOutputCapabilities(pause=False), sample_rate=None)
        self.bytes = 0
        self.frames = 0
        self.first = None
        self.t0 = time.monotonic()
        self._seg = False

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await super().capture_frame(frame)
        self._seg = True
        self.bytes += len(bytes(frame.data))
        self.frames += 1
        if self.first is None:
            self.first = time.monotonic() - self.t0
            print(f"  first agent audio after {self.first:.2f}s (sr={frame.sample_rate})")

    def flush(self) -> None:
        super().flush()
        if self._seg:
            self._seg = False
            self.on_playback_finished(playback_position=self.bytes / 48000, interrupted=False)

    def clear_buffer(self) -> None:
        if self._seg:
            self._seg = False
            self.on_playback_finished(playback_position=0, interrupted=True)


async def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=os.environ.get("DEFAULT_REALTIME_MODEL", "gemini-3.1-flash-live-preview"))
    ap.add_argument("--seconds", type=float, default=12.0)
    ap.add_argument("--trigger", default="direct", choices=["direct", "queue", "generate"])
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for noisy in ("httpx", "httpcore", "websockets", "asyncio", "google_genai"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    from engine.config import Settings
    from engine.schemas.agent import AgentConfig, Mode, VoiceRef
    from engine.worker.graph.runtime import realtime_session, trigger_text
    from engine.worker.pipeline import make_session

    settings = Settings()
    cfg = AgentConfig(name="rtcheck", mode=Mode.s2s, instructions="You are a test agent. One short sentence.",
                      llm=None, realtime=VoiceRef(provider="google", model=args.model, voice="Aoede", language="en-IN"))
    session: AgentSession = make_session(cfg, settings, prewarmed={"vad": None})
    out = CountingOutput()
    session.input.audio = SilenceInput()
    session.output.audio = out

    for ev in ("agent_state_changed", "user_state_changed", "conversation_item_added", "error", "speech_created",
               "session_usage_updated"):
        session.on(ev, lambda e, ev=ev: print(f"  event {ev}: {str(e)[:160]}"))

    class A(Agent):
        def __init__(self):
            super().__init__(instructions="You are a test agent. Greet the caller in one short sentence.")

        async def on_enter(self):
            if args.trigger == "generate":
                self.session.generate_reply(instructions="Greet the caller now.")
                return
            if args.trigger == "queue":
                ok = trigger_text(self.session, "[Call connected. Greet the caller now.]")
                print("  queued trigger:", ok)
                return
            # direct: wait for the underlying genai session and send on it
            rt = realtime_session(self.session)
            for _ in range(100):
                active = getattr(rt, "_active_session", None)
                if active is not None:
                    break
                await asyncio.sleep(0.05)
            print("  active genai session:", active is not None)
            if active is not None:
                from google.genai import types
                await active.send_client_content(
                    turns=[types.Content(role="user", parts=[types.Part(text="[Call connected. Greet the caller now.]")])],
                    turn_complete=True)
                print("  direct trigger sent")

    await session.start(agent=A())
    await asyncio.sleep(args.seconds)
    await session.aclose()
    print(f"RESULT model={args.model} trigger={args.trigger}: frames={out.frames} bytes={out.bytes} "
          f"(~{out.bytes / 48000:.2f}s @24k) first={out.first}")
    return 0 if out.bytes > 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
