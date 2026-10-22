"""Call recording: tee both audio directions into one mono 8 kHz WAV (caller + agent mixed).

Wraps the transport's AudioInput/AudioOutput so recording never touches the real-time path's
error handling: every mixer call is guarded and a failure only loses the recording.
"""

from __future__ import annotations

import array
import logging
import os
import time
import wave
from typing import Optional

from livekit import rtc
from livekit.agents.voice.io import AudioInput, AudioOutput, AudioOutputCapabilities

from engine.worker.audio import resample_pcm16

try:
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

logger = logging.getLogger(__name__)
RATE = 8000


class Mixer:
    def __init__(self, max_seconds: int = 900):
        self._t0 = time.monotonic()
        self._buf = array.array("h")
        self._max = max_seconds * RATE
        self._agent_cursor = 0            # sample index where the next agent frame goes
        self.frames_in = 0
        self.frames_out = 0

    def _now_idx(self) -> int:
        return int((time.monotonic() - self._t0) * RATE)

    def _place(self, pcm8k: bytes, start: int) -> None:
        n = len(pcm8k) // 2
        if n == 0 or start > self._max:
            return
        buf = self._buf
        if len(buf) < start:
            buf.frombytes(bytes(2 * (start - len(buf))))
        if _np is not None:
            new = _np.frombuffer(pcm8k[: n * 2], dtype=_np.int16)
            ov = min(len(buf) - start, n) if start < len(buf) else 0
            if ov > 0:
                old = _np.frombuffer(buf, dtype=_np.int16, count=ov, offset=start * 2).astype(_np.int32)
                mixed = _np.clip(old + new[:ov].astype(_np.int32), -32768, 32767).astype(_np.int16)
                buf[start:start + ov] = array.array("h", mixed.tobytes())
            if ov < n:
                buf.frombytes(new[ov:].tobytes())
            return
        import struct
        samples = struct.unpack(f"<{n}h", pcm8k[: n * 2])
        for i, s in enumerate(samples):
            idx = start + i
            if idx < len(buf):
                v = buf[idx] + s
                buf[idx] = 32767 if v > 32767 else (-32768 if v < -32768 else v)
            else:
                buf.append(s)

    def add_caller(self, pcm: bytes, rate: int) -> None:
        try:
            self._place(resample_pcm16(pcm, rate, RATE), self._now_idx())
            self.frames_in += 1
        except Exception:
            pass

    def add_agent(self, pcm: bytes, rate: int) -> None:
        try:
            pcm8 = resample_pcm16(pcm, rate, RATE)
            start = max(self._agent_cursor, self._now_idx())
            self._place(pcm8, start)
            self._agent_cursor = start + len(pcm8) // 2
            self.frames_out += 1
        except Exception:
            pass

    def agent_interrupted(self) -> None:
        """Playout was cleared: drop the not-yet-played tail of the agent track."""
        try:
            now = self._now_idx()
            if self._agent_cursor > now:
                del self._buf[now:]
                self._agent_cursor = now
        except Exception:
            pass

    def write(self, path: str) -> Optional[str]:
        if not self._buf:
            return None
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
            with wave.open(path, "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(RATE)
                wf.writeframes(self._buf.tobytes())
            return path
        except Exception as e:
            logger.warning("failed to write recording %s: %s", path, e)
            return None


class TeeAudioInput(AudioInput):
    def __init__(self, source: AudioInput, mixer: Mixer):
        super().__init__(label="RecordingTee", source=source)
        self._mixer = mixer

    async def __anext__(self) -> rtc.AudioFrame:
        frame = await super().__anext__()
        self._mixer.add_caller(bytes(frame.data), frame.sample_rate)
        return frame


class RecordingAudioOutput(AudioOutput):
    def __init__(self, next_in_chain: AudioOutput, mixer: Mixer):
        super().__init__(label="RecordingTee", capabilities=AudioOutputCapabilities(pause=True),
                         next_in_chain=next_in_chain, sample_rate=next_in_chain.sample_rate)
        self._mixer = mixer

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await super().capture_frame(frame)
        self._mixer.add_agent(bytes(frame.data), frame.sample_rate)
        assert self.next_in_chain is not None
        await self.next_in_chain.capture_frame(frame)

    def flush(self) -> None:
        super().flush()
        assert self.next_in_chain is not None
        self.next_in_chain.flush()

    def clear_buffer(self) -> None:
        self._mixer.agent_interrupted()
        assert self.next_in_chain is not None
        self.next_in_chain.clear_buffer()
