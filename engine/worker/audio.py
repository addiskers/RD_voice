"""Telephony audio codecs and resampling (pure Python + optional numpy fast path).

Carriers stream G.711 mu-law at 8 kHz in 20 ms frames (160 bytes). The conversation runtime
works in PCM16 at 16 kHz (input) and whatever the TTS/realtime model emits (16/24/48 kHz) on
output. Tables are built once at import; ported from the production EO bridge.
"""

from __future__ import annotations

import array
import struct

try:
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

FRAME_BYTES = 160            # 20 ms of mu-law @ 8 kHz
FRAME_S = 0.020
CARRIER_RATE = 8000
PIPELINE_RATE = 16000

# --- G.711 mu-law tables ---------------------------------------------------------------------
_MULAW_DECODE = array.array("h")
for _i in range(256):
    _v = ~_i
    _sign = _v & 0x80
    _exponent = (_v >> 4) & 0x07
    _mantissa = _v & 0x0F
    _sample = ((_mantissa << 3) + 0x84) << _exponent
    _sample -= 0x84
    if _sign:
        _sample = -_sample
    _MULAW_DECODE.append(max(-32768, min(32767, _sample)))

_MULAW_BIAS = 0x84
_MULAW_CLIP = 32635
_MULAW_EXP_TABLE = [0, 0, 1, 1, 2, 2, 2, 2] + [3] * 8 + [4] * 16 + [5] * 32 + [6] * 64 + [7] * 128


def _pcm16_to_mulaw_sample(sample: int) -> int:
    sign = 0
    if sample < 0:
        sign = 0x80
        sample = -sample
    if sample > _MULAW_CLIP:
        sample = _MULAW_CLIP
    sample += _MULAW_BIAS
    exponent = _MULAW_EXP_TABLE[(sample >> 7) & 0xFF]
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return ~(sign | (exponent << 4) | mantissa) & 0xFF


_PCM_TO_ULAW = bytes(_pcm16_to_mulaw_sample(u - 65536 if u >= 32768 else u) for u in range(65536))
_MULAW_SQ = [int(v) * int(v) for v in _MULAW_DECODE]

if _np is not None:
    _MULAW_DECODE_NP = _np.array(_MULAW_DECODE, dtype=_np.int16)
    _MULAW_SQ_NP = _np.array(_MULAW_SQ, dtype=_np.int64)
    _PCM_TO_ULAW_NP = _np.frombuffer(_PCM_TO_ULAW, dtype=_np.uint8)


# --- decode / encode ---------------------------------------------------------------------------
def mulaw_to_pcm16(mulaw: bytes) -> bytes:
    """mu-law 8 kHz -> PCM16 8 kHz (no resampling)."""
    if not mulaw:
        return b""
    if _np is not None:
        return _MULAW_DECODE_NP[_np.frombuffer(mulaw, dtype=_np.uint8)].tobytes()
    return struct.pack(f"<{len(mulaw)}h", *(_MULAW_DECODE[b] for b in mulaw))


def mulaw_to_pcm16k(mulaw: bytes) -> bytes:
    """mu-law 8 kHz -> PCM16 16 kHz (linear interpolation upsample)."""
    if not mulaw:
        return b""
    if _np is not None:
        s8 = _MULAW_DECODE_NP[_np.frombuffer(mulaw, dtype=_np.uint8)]
        nxt = _np.append(s8[1:], s8[-1]).astype(_np.int32)
        mid = ((s8.astype(_np.int32) + nxt) >> 1).astype(_np.int16)
        out = _np.empty(s8.size * 2, dtype=_np.int16)
        out[0::2] = s8
        out[1::2] = mid
        return out.tobytes()
    s8 = [_MULAW_DECODE[b] for b in mulaw]
    out = []
    n = len(s8)
    for i in range(n):
        out.append(s8[i])
        out.append((s8[i] + s8[i + 1]) >> 1 if i + 1 < n else s8[i])
    return struct.pack(f"<{len(out)}h", *out)


def pcm16_to_mulaw(pcm: bytes, rate: int) -> bytes:
    """PCM16 mono at ``rate`` (8k/16k/24k/48k) -> mu-law 8 kHz, with a box-average low-pass when
    downsampling so the agent's voice does not alias into a metallic tone."""
    if rate % CARRIER_RATE:
        raise ValueError(f"unsupported rate {rate}; need a multiple of 8000")
    k = rate // CARRIER_RATE
    n_samples = len(pcm) // 2
    if n_samples == 0:
        return b""
    if _np is not None:
        s = _np.frombuffer(pcm[: n_samples * 2], dtype=_np.int16)
        if k == 1:
            return _PCM_TO_ULAW_NP[s.astype(_np.uint16)].tobytes()
        m = s.size // k
        if m == 0:
            return b""
        avg = s[: m * k].astype(_np.int32).reshape(m, k).sum(axis=1) // k
        return _PCM_TO_ULAW_NP[avg.astype(_np.int64) & 0xFFFF].tobytes()
    samples = struct.unpack(f"<{n_samples}h", pcm[: n_samples * 2])
    lut = _PCM_TO_ULAW
    if k == 1:
        return bytes(lut[s & 0xFFFF] for s in samples)
    return bytes(lut[(sum(samples[i:i + k]) // k) & 0xFFFF] for i in range(0, n_samples - k + 1, k))


def mulaw_frame_meansquare(mulaw: bytes) -> float:
    """Cheap energy of one inbound frame (mean of squared PCM16) for a real-time voice gate."""
    n = len(mulaw)
    if not n:
        return 0.0
    if _np is not None:
        return float(_MULAW_SQ_NP[_np.frombuffer(mulaw, dtype=_np.uint8)].mean())
    return sum(map(_MULAW_SQ.__getitem__, mulaw)) / n


# --- generic PCM16 resampling (browser transport, recording mix) ------------------------------
def resample_pcm16(pcm: bytes, src_rate: int, dst_rate: int) -> bytes:
    """Linear-interpolation resample of mono PCM16. Good enough for speech; symmetric ratios only
    need to be integer-friendly, arbitrary ratios are handled by interpolation."""
    if src_rate == dst_rate or not pcm:
        return pcm
    n = len(pcm) // 2
    if n == 0:
        return b""
    if _np is not None:
        s = _np.frombuffer(pcm[: n * 2], dtype=_np.int16).astype(_np.float32)
        m = int(round(n * dst_rate / src_rate))
        if m <= 0:
            return b""
        x_old = _np.linspace(0.0, 1.0, num=n, endpoint=False)
        x_new = _np.linspace(0.0, 1.0, num=m, endpoint=False)
        out = _np.interp(x_new, x_old, s)
        return _np.clip(out, -32768, 32767).astype(_np.int16).tobytes()
    samples = struct.unpack(f"<{n}h", pcm[: n * 2])
    m = int(round(n * dst_rate / src_rate))
    out = []
    for j in range(m):
        pos = j * n / m
        i = int(pos)
        frac = pos - i
        a = samples[i]
        b = samples[i + 1] if i + 1 < n else a
        out.append(int(a + (b - a) * frac))
    return struct.pack(f"<{len(out)}h", *out)


def pcm16_energy(pcm: bytes) -> float:
    """Mean square of PCM16 samples (browser/VAD gate)."""
    n = len(pcm) // 2
    if n == 0:
        return 0.0
    if _np is not None:
        s = _np.frombuffer(pcm[: n * 2], dtype=_np.int16).astype(_np.int64)
        return float((s * s).mean())
    samples = struct.unpack(f"<{n}h", pcm[: n * 2])
    return sum(v * v for v in samples) / n


SILENCE_FRAME_ULAW = bytes([0xFF]) * FRAME_BYTES     # 20 ms of digital silence in mu-law
