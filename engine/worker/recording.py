"""Call recording: two channels (left = caller, right = agent) at 8 kHz, written to disk as the call runs.

Each direction is appended to its own raw PCM file at its wall-clock position (gaps are zero-filled, a
burst never overwrites), so memory stays at one frame regardless of call length. ``finish`` interleaves
the two raw files into one stereo WAV. Barge-in truncates the agent track at "now": audio the caller never
heard is not in the recording. Every call is guarded: a recording failure only loses the recording.
"""

from __future__ import annotations

import logging
import os
import time
import wave
from typing import Optional

from livekit import rtc
from livekit.agents.voice.io import AudioInput, AudioOutput, AudioOutputCapabilities

from engine.worker.audio import StreamResampler

try:
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

logger = logging.getLogger(__name__)
RATE = 8000
RAW_SUFFIXES = (".caller.raw", ".agent.raw")


class _Track:
    """One direction: append-only raw PCM16 at 8 kHz, positioned by time."""

    def __init__(self, path: str, t0: float, max_samples: int):
        self.path = path
        self._t0 = t0
        self._max = max_samples
        self._f = open(path, "wb")
        self.cursor = 0                  # samples written
        self._rs: dict[int, StreamResampler] = {}
        self.frames = 0

    def now_idx(self) -> int:
        return int((time.monotonic() - self._t0) * RATE)

    def _resample(self, pcm: bytes, rate: int) -> bytes:
        if rate == RATE:
            return pcm
        rs = self._rs.get(rate)
        if rs is None:
            rs = self._rs[rate] = StreamResampler(rate, RATE)
        return rs.process(pcm)

    def write(self, pcm: bytes, rate: int, *, at: Optional[int] = None) -> None:
        pcm8 = self._resample(pcm, rate)
        if not pcm8 or self.cursor >= self._max:
            return
        start = max(self.cursor, self.now_idx() if at is None else at)
        if start > self.cursor:
            self._f.write(bytes(2 * (start - self.cursor)))     # silence for the gap
            self.cursor = start
        n = len(pcm8) // 2
        self._f.write(pcm8[: n * 2])
        self.cursor += n
        self.frames += 1

    def truncate_to_now(self) -> None:
        now = self.now_idx()
        if self.cursor > now:
            self._f.flush()
            self._f.truncate(now * 2)
            self._f.seek(now * 2)
            self.cursor = now

    def close(self) -> bytes:
        tail = b"".join(rs.flush() for rs in self._rs.values())
        if tail and self.cursor < self._max:
            self._f.write(tail)
            self.cursor += len(tail) // 2
        self._f.close()
        with open(self.path, "rb") as f:
            return f.read()


class StereoRecorder:
    def __init__(self, base_path: str, max_seconds: int = 900):
        os.makedirs(os.path.dirname(os.path.abspath(base_path)) or ".", exist_ok=True)
        self.base_path = base_path
        self._t0 = time.monotonic()
        max_samples = max_seconds * RATE
        self._caller = _Track(base_path + RAW_SUFFIXES[0], self._t0, max_samples)
        self._agent = _Track(base_path + RAW_SUFFIXES[1], self._t0, max_samples)
        self._finished = False

    @property
    def frames_in(self) -> int:
        return self._caller.frames

    @property
    def frames_out(self) -> int:
        return self._agent.frames

    def add_caller(self, pcm: bytes, rate: int) -> None:
        try:
            self._caller.write(pcm, rate)
        except Exception as e:
            logger.debug("recording caller frame failed: %s", e)

    def add_agent(self, pcm: bytes, rate: int) -> None:
        try:
            self._agent.write(pcm, rate)      # at max(cursor, now): queued playout stays contiguous
        except Exception as e:
            logger.debug("recording agent frame failed: %s", e)

    def agent_interrupted(self) -> None:
        """Playout was cleared: drop the not-yet-played tail of the agent track."""
        try:
            self._agent.truncate_to_now()
        except Exception as e:
            logger.debug("recording truncate failed: %s", e)

    def finish(self) -> Optional[str]:
        if self._finished:
            return None
        self._finished = True
        try:
            left, right = self._caller.close(), self._agent.close()
        except Exception as e:
            logger.warning("failed to close recording tracks for %s: %s", self.base_path, e)
            return None
        for suffix in RAW_SUFFIXES:
            try:
                os.remove(self.base_path + suffix)
            except OSError:
                pass
        if not left and not right:
            return None
        n = max(len(left), len(right)) // 2
        try:
            if _np is not None:
                l_arr = _np.frombuffer(left.ljust(n * 2, b"\0"), dtype=_np.int16)
                r_arr = _np.frombuffer(right.ljust(n * 2, b"\0"), dtype=_np.int16)
                stereo = _np.stack([l_arr, r_arr], axis=1).reshape(-1).tobytes()
            else:  # pragma: no cover
                import array
                l_arr = array.array("h", left.ljust(n * 2, b"\0"))
                r_arr = array.array("h", right.ljust(n * 2, b"\0"))
                out = array.array("h")
                for a, b in zip(l_arr, r_arr):
                    out.append(a)
                    out.append(b)
                stereo = out.tobytes()
            path = self.base_path + ".wav"
            with wave.open(path, "wb") as wf:
                wf.setnchannels(2)
                wf.setsampwidth(2)
                wf.setframerate(RATE)
                wf.writeframes(stereo)
            return path
        except Exception as e:
            logger.warning("failed to write recording %s: %s", self.base_path, e)
            return None


def remove_stale_raw(directory: str) -> int:
    """Delete raw track files a crashed worker left behind (they are useless without their pair's timing)."""
    n = 0
    if not os.path.isdir(directory):
        return 0
    for name in os.listdir(directory):
        if name.endswith(RAW_SUFFIXES):
            try:
                os.remove(os.path.join(directory, name))
                n += 1
            except OSError:
                pass
    return n


class TeeAudioInput(AudioInput):
    def __init__(self, source: AudioInput, recorder: StereoRecorder):
        super().__init__(label="RecordingTee", source=source)
        self._rec = recorder

    async def __anext__(self) -> rtc.AudioFrame:
        frame = await super().__anext__()
        self._rec.add_caller(bytes(frame.data), frame.sample_rate)
        return frame


class RecordingAudioOutput(AudioOutput):
    def __init__(self, next_in_chain: AudioOutput, recorder: StereoRecorder):
        super().__init__(label="RecordingTee", capabilities=AudioOutputCapabilities(pause=True),
                         next_in_chain=next_in_chain, sample_rate=next_in_chain.sample_rate)
        self._rec = recorder

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await super().capture_frame(frame)
        self._rec.add_agent(bytes(frame.data), frame.sample_rate)
        assert self.next_in_chain is not None
        await self.next_in_chain.capture_frame(frame)

    def flush(self) -> None:
        super().flush()
        assert self.next_in_chain is not None
        self.next_in_chain.flush()

    def clear_buffer(self) -> None:
        self._rec.agent_interrupted()
        assert self.next_in_chain is not None
        self.next_in_chain.clear_buffer()
