"""Two-channel 8 kHz recordings written during the call, object storage backends, retention sweep."""

import math
import struct
import wave
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from engine.storage import LocalStorage, build_storage, recording_key
from engine.worker.recording import StereoRecorder, remove_stale_raw
from conftest import H


def _sine(rate: int, seconds: float, freq: float, amp: int = 8000) -> bytes:
    n = int(rate * seconds)
    return struct.pack(f"<{n}h", *(int(amp * math.sin(2 * math.pi * freq * i / rate)) for i in range(n)))


def _read_wav(path: str):
    with wave.open(path, "rb") as wf:
        assert wf.getsampwidth() == 2
        data = np.frombuffer(wf.readframes(wf.getnframes()), dtype=np.int16)
        return wf.getnchannels(), wf.getframerate(), data.reshape(-1, wf.getnchannels())


def _peak_hz(samples: np.ndarray, rate: int = 8000) -> float:
    spectrum = np.abs(np.fft.rfft(samples.astype(np.float64)))
    return float(np.fft.rfftfreq(len(samples), d=1 / rate)[int(np.argmax(spectrum[1:])) + 1])


def test_stereo_recorder_writes_two_channels(tmp_path):
    rec = StereoRecorder(str(tmp_path / "call_a"), max_seconds=10)
    rec.add_caller(_sine(16000, 0.5, 1000.0), 16000)
    rec.add_agent(_sine(24000, 0.5, 440.0), 24000)
    path = rec.finish()
    assert path == str(tmp_path / "call_a.wav")
    channels, rate, data = _read_wav(path)
    assert channels == 2 and rate == 8000 and 3900 <= len(data) <= 4200
    left, right = data[:, 0], data[:, 1]
    assert np.sqrt(np.mean(left.astype(float) ** 2)) > 1000 and np.sqrt(np.mean(right.astype(float) ** 2)) > 1000
    assert abs(_peak_hz(left) - 1000) < 20 and abs(_peak_hz(right) - 440) < 20
    assert not (tmp_path / "call_a.caller.raw").exists() and not (tmp_path / "call_a.agent.raw").exists()


def test_agent_interrupt_truncates_tail(tmp_path):
    rec = StereoRecorder(str(tmp_path / "call_b"), max_seconds=10)
    rec.add_agent(_sine(8000, 2.0, 440.0), 8000)          # 2 s queued for playout
    rec.agent_interrupted()                                  # barge-in right away: nothing after "now" was heard
    path = rec.finish()
    _, _, data = _read_wav(path)
    assert len(data) < 2000                                  # well under the 16000 samples that were queued


def test_finish_tolerates_missing_agent_file(tmp_path):
    rec = StereoRecorder(str(tmp_path / "call_c"), max_seconds=10)
    rec.add_caller(_sine(8000, 0.3, 500.0), 8000)
    path = rec.finish()
    channels, _, data = _read_wav(path)
    assert channels == 2 and np.all(data[:, 1] == 0) and np.any(data[:, 0] != 0)


def test_finish_returns_none_when_empty(tmp_path):
    assert StereoRecorder(str(tmp_path / "call_d"), max_seconds=10).finish() is None


def test_stale_raw_files_removed_on_start(tmp_path):
    (tmp_path / "x.caller.raw").write_bytes(b"\0" * 10)
    (tmp_path / "y.agent.raw").write_bytes(b"\0" * 10)
    (tmp_path / "keep.wav").write_bytes(b"RIFF")
    assert remove_stale_raw(str(tmp_path)) == 2 and (tmp_path / "keep.wav").exists()


@pytest.mark.asyncio
async def test_local_storage_put_get_delete(tmp_path):
    src = tmp_path / "src.wav"
    src.write_bytes(b"RIFFdata")
    st = LocalStorage(str(tmp_path / "bucket"))
    key = "recordings/2026/10/22/call_a.wav"
    await st.put_file(str(src), key)
    assert await st.exists(key) and await st.get_bytes(key) == b"RIFFdata"
    assert st.signed_url(key) is None
    await st.delete(key)
    assert not await st.exists(key)


def test_recording_key():
    assert recording_key("call_a", datetime(2026, 10, 22, 5, 0, tzinfo=timezone.utc)) == "recordings/2026/10/22/call_a.wav"


def test_build_storage_local_default():
    from engine.config import Settings
    st = build_storage(Settings(_env_file=None, recordings_dir="./data/recordings"))
    assert isinstance(st, LocalStorage)


def _insert_call_with_recording(client, key: str, days_old: int) -> str:
    from engine.api.db import Call
    engine = client.app.state.engine

    async def go():
        async with engine.db.session() as s:
            call = Call(id="call_rec", status="completed", direction="outbound", provider="plivo", to_number="+91",
                        recording_path=key, ended_emitted=True,
                        ended_at=datetime.now(timezone.utc) - timedelta(days=days_old))
            s.add(call)
            await s.commit()
    client.portal.call(go)
    return "call_rec"


def test_recording_endpoint_streams_from_storage(client, tmp_path):
    engine = client.app.state.engine
    key = recording_key("call_rec", datetime.now(timezone.utc))
    src = tmp_path / "r.wav"
    src.write_bytes(b"RIFF-two-channel-bytes")
    client.portal.call(lambda: engine.storage.put_file(str(src), key))
    call_id = _insert_call_with_recording(client, key, days_old=1)
    r = client.get(f"/v1/calls/{call_id}/recording", headers=H)
    assert r.status_code == 200 and r.headers["content-type"].startswith("audio/wav") and r.content == b"RIFF-two-channel-bytes"


def test_retention_sweep_deletes_old(client, tmp_path):
    engine = client.app.state.engine
    key = recording_key("call_rec", datetime.now(timezone.utc) - timedelta(days=100))
    src = tmp_path / "old.wav"
    src.write_bytes(b"old")
    client.portal.call(lambda: engine.storage.put_file(str(src), key))
    call_id = _insert_call_with_recording(client, key, days_old=100)

    async def sweep():
        async with engine.db.session() as s:
            return await engine.service.sweep_recordings(s, engine.storage)

    assert client.portal.call(sweep) == 1
    assert client.portal.call(lambda: engine.storage.exists(key)) is False
    assert client.get(f"/v1/calls/{call_id}/recording", headers=H).status_code == 404
    assert client.get(f"/v1/calls/{call_id}", headers=H).json()["recording_url"] is None
